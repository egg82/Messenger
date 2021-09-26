package ninja.egg82.messenger;

import com.rabbitmq.client.*;
import io.netty.buffer.ByteBuf;
import ninja.egg82.messenger.core.Pair;
import ninja.egg82.messenger.handler.MessagingHandler;
import ninja.egg82.messenger.packets.Packet;
import ninja.egg82.messenger.packets.server.KeepAlivePacket;
import ninja.egg82.messenger.packets.server.PacketVersionRequestPacket;
import ninja.egg82.messenger.services.CollectionProvider;
import ninja.egg82.messenger.services.PacketService;
import ninja.egg82.messenger.utils.ValidationUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RabbitMQMessagingService extends AbstractMessagingService {
    // https://www.rabbitmq.com/api-guide.html#recovery
    private ConnectionFactory factory;
    // "Connection" acts as our pool
    // https://stackoverflow.com/questions/10407760/is-there-a-performance-difference-between-pooling-connections-or-channels-in-rab
    private RecoverableConnection connection;

    private volatile boolean closed = false;
    private final ReadWriteLock queueLock = new ReentrantReadWriteLock();

    private final String exchangeName;

    private RabbitMQMessagingService(@NotNull PacketService packetService, @NotNull String name, @NotNull String channelName, long startupDelay, boolean dumpPackets, @NotNull File packetDirectory) {
        super(packetService, name, startupDelay, dumpPackets, packetDirectory);
        this.exchangeName = channelName;
    }

    @Override
    public void close() {
        queueLock.writeLock().lock();
        try {
            closed = true;
            try {
                connection.close(8000);
            } catch (IOException ignored) { }
            packetService.removeMessenger(this);
        } finally {
            queueLock.writeLock().unlock();
        }
    }

    @Override
    public boolean isClosed() { return closed || !connection.isOpen(); }

    public static @NotNull Builder builder(
            @NotNull PacketService packetService,
            @NotNull String name,
            @NotNull String channelName,
            @NotNull UUID serverId,
            @NotNull MessagingHandler handler,
            long startupDelay,
            boolean dumpPackets,
            @NotNull File packetDirectory
    ) { return new Builder(packetService, name, channelName, serverId, handler, startupDelay, dumpPackets, packetDirectory); }

    public static class Builder {
        private final RabbitMQMessagingService service;
        private final ConnectionFactory config = new ConnectionFactory();

        public Builder(@NotNull PacketService packetService, @NotNull String name, @NotNull String channelName, @NotNull UUID serverId, @NotNull MessagingHandler handler, long startupDelay, boolean dumpPackets, @NotNull File packetDirectory) {
            service = new RabbitMQMessagingService(packetService, name, channelName, startupDelay, dumpPackets, packetDirectory);
            service.serverId = serverId;
            service.serverIdString = serverId.toString();
            ByteBuf buffer = alloc.buffer(16, 16);
            try {
                buffer.writeLong(serverId.getMostSignificantBits());
                buffer.writeLong(serverId.getLeastSignificantBits());
                if (buffer.isDirect()) {
                    service.serverIdBytes = new byte[16];
                    buffer.readBytes(service.serverIdBytes);
                } else {
                    service.serverIdBytes = buffer.array();
                }
            } finally {
                buffer.release();
            }

            service.handler = handler;

            config.setAutomaticRecoveryEnabled(true);
            config.setTopologyRecoveryEnabled(true);
        }

        public @NotNull Builder url(@NotNull String address, int port, @NotNull String vhost) {
            config.setHost(address);
            config.setPort(port);
            config.setVirtualHost(vhost);
            return this;
        }

        public @NotNull Builder credentials(@NotNull String user, @NotNull String pass) {
            config.setUsername(user);
            config.setPassword(pass);
            return this;
        }

        public @NotNull Builder timeout(int timeout) {
            config.setConnectionTimeout(timeout);
            return this;
        }

        public @NotNull RabbitMQMessagingService build() throws IOException, TimeoutException {
            service.factory = config;
            service.connection = service.getConnection();
            if (service.startupDelay == 0L) {
                service.bind();
                service.packetService.addMessenger(service);
            } else {
                CompletableFuture.runAsync(() -> {
                    try {
                        Thread.sleep(service.startupDelay);
                    } catch (InterruptedException ex) {
                        service.logger.error(ex.getClass().getName() + ": " + ex.getMessage(), ex);
                        Thread.currentThread().interrupt();
                    }
                }).thenRun(() -> {
                    try {
                        service.bind();
                        service.packetService.addMessenger(service);
                    } catch (IOException ex) {
                        service.logger.error(ex.getClass().getName() + ": " + ex.getMessage(), ex);
                        throw new CompletionException(ex);
                    }
                });
            }
            return service;
        }
    }

    private void bind() throws IOException {
        RecoverableChannel channel = getChannel();
        channel.exchangeDeclare(exchangeName, ExchangeType.FANOUT.getType(), true);
        String queue = channel.queueDeclare().getQueue();
        channel.queueBind(queue, exchangeName, "");
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String tag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                UUID sender = validateProperties(properties);
                if (sender == null) {
                    return;
                }

                byte packetVersion = CollectionProvider.getServerVersions().getOrDefault(sender, (byte) -1);
                if (packetVersion > -1 && packetVersion != packetService.getPacketVersion()) {
                    logger.warn("Server " + sender + " packet version " + String.format(
                            "0x%02X ",
                            packetVersion
                    ) + " does not match current packet version " + String.format("0x%02X ", packetService.getPacketVersion()) + ". Skipping packet.");
                    return;
                }

                ByteBuf b = alloc.buffer(body.length, body.length);
                ByteBuf data = null;
                try {
                    b.writeBytes(body);
                    data = decompressData(b);

                    if (dumpPackets) {
                        dumpReceivedPacket(data);
                    }

                    byte packetId = data.readByte();
                    Packet packet;
                    try {
                        packet = PacketManager.read(packetId, sender, data);
                        if (packet == null) {
                            logger.warn("Received packet ID that doesn't exist: " + packetId);
                            return;
                        }
                    } catch (Exception ex) {
                        Class<? extends Packet> clazz = PacketManager.getPacket(packetId);
                        logger.error(
                                "Could not instantiate packet" + (clazz != null ? clazz.getName() : "null"),
                                ex
                        );
                        return;
                    }

                    if (packetVersion == -1 && packet instanceof KeepAlivePacket) {
                        // Don't send warning
                        return;
                    }

                    if (packetVersion == -1 && !hasVersion(packet)) {
                        logger.warn("Server " + sender + " packet version is unknown, and packet type is of " + packet.getClass().getName() + ". Skipping packet.");
                        // There's a potential race condition here with double-sending a request, but it doesn't really matter
                        ByteBuf finalData = data;
                        CollectionProvider.getPacketProcessingQueue().compute(sender, (k, v) -> {
                            if (v == null) {
                                v = new CopyOnWriteArrayList<>();
                            }

                            if (v.isEmpty()) {
                                if (packet.verifyFullRead(finalData)) {
                                    v.add(new Pair<>(UUID.fromString(properties.getMessageId()), packet));
                                }
                                packetService.queuePacket(new PacketVersionRequestPacket(sender, serverId));
                            } else {
                                if (packet.verifyFullRead(finalData)) {
                                    v.add(new Pair<>(UUID.fromString(properties.getMessageId()), packet));
                                }
                            }

                            return v;
                        });
                        return;
                    }

                    if (packet.verifyFullRead(data)) {
                        handler.handlePacket(UUID.fromString(properties.getMessageId()), getName(), packet);
                    }
                } finally {
                    b.release();
                    if (data != null) {
                        data.release();
                    }
                }
            }
        };
        channel.addShutdownListener(cause -> {
            try {
                bind();
            } catch (IOException ex) {
                logger.error("Could not re-bind channel.", ex);
            }
        });
        channel.basicConsume(queue, true, consumer);
    }

    @Override
    public void sendPacket(@NotNull UUID messageId, @NotNull Packet packet) throws IOException, TimeoutException {
        queueLock.readLock().lock();
        try (RecoverableChannel channel = getChannel()) {
            ByteBuf buffer = alloc.buffer(getInitialCapacity());
            try {
                buffer.writeByte(PacketManager.getId(packet.getClass()));
                packet.write(buffer);
                addCapacity(buffer.writerIndex());

                if (dumpPackets) {
                    dumpSentPacket(buffer);
                }

                AMQP.BasicProperties properties = getProperties(DeliveryMode.PERSISTENT, messageId);
                channel.exchangeDeclare(exchangeName, ExchangeType.FANOUT.getType(), true);
                channel.basicPublish(exchangeName, "", properties, compressData(buffer));
            } finally {
                buffer.release();
            }
        } finally {
            queueLock.readLock().unlock();
        }
    }

    private @NotNull AMQP.BasicProperties getProperties(@NotNull DeliveryMode deliveryMode, @NotNull UUID messageId) {
        Map<String, Object> headers = new HashMap<>();
        headers.put("sender", serverIdBytes);

        AMQP.BasicProperties.Builder retVal = new AMQP.BasicProperties.Builder();
        retVal.contentType("application/octet-stream");
        retVal.messageId(messageId.toString());
        retVal.deliveryMode(deliveryMode.getMode());
        retVal.headers(headers);
        return retVal.build();
    }

    private @Nullable UUID validateProperties(@NotNull AMQP.BasicProperties properties) {
        byte[] data = (byte[]) properties.getHeaders().get("sender");
        ByteBuf buffer = alloc.buffer(16, 16);
        UUID sender;
        try {
            buffer.writeBytes(data);
            sender = new UUID(buffer.readLong(), buffer.readLong());
        } finally {
            buffer.release();
        }
        if (serverId.equals(sender)) {
            return null;
        }
        if (!ValidationUtil.isValidUuid(properties.getMessageId())) {
            logger.warn("Non-valid message ID received: \"" + properties.getMessageId() + "\".");
            return null;
        }
        return sender;
    }

    private @NotNull RecoverableConnection getConnection() throws IOException, TimeoutException { return (RecoverableConnection) factory.newConnection(); }

    private @NotNull RecoverableChannel getChannel() throws IOException { return (RecoverableChannel) connection.createChannel(); }

    private enum DeliveryMode {
        /**
         * Not logged to disk
         */
        TRANSIENT(1),
        /**
         * When in a durable exchange, logged to disk
         */
        PERSISTENT(2);

        private final int mode;

        DeliveryMode(int mode) { this.mode = mode; }

        public int getMode() { return mode; }
    }

    private enum ExchangeType {
        DIRECT("direct"),
        FANOUT("fanout"),
        TOPIC("topic"),
        HEADERS("match"); // AMQP compatibility

        private final String type;

        ExchangeType(@NotNull String type) { this.type = type; }

        public @NotNull String getType() { return type; }
    }
}
