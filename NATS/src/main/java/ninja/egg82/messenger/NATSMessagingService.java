package ninja.egg82.messenger;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.netty.buffer.ByteBuf;
import ninja.egg82.messenger.core.Pair;
import ninja.egg82.messenger.handler.MessagingHandler;
import ninja.egg82.messenger.packets.Packet;
import ninja.egg82.messenger.packets.server.KeepAlivePacket;
import ninja.egg82.messenger.packets.server.PacketVersionRequestPacket;
import ninja.egg82.messenger.services.CollectionProvider;
import ninja.egg82.messenger.services.PacketService;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class NATSMessagingService extends AbstractMessagingService {
    private Connection connection;
    private Dispatcher dispatcher;

    private volatile boolean closed = false;
    private final ReadWriteLock queueLock = new ReentrantReadWriteLock();

    private final String subjectName;

    private NATSMessagingService(@NotNull PacketService packetService, @NotNull String name, @NotNull String channelName, long startupDelay, boolean dumpPackets, @NotNull File packetDirectory) {
        super(packetService, name, startupDelay, dumpPackets, packetDirectory);
        this.subjectName = channelName;
    }

    @Override
    public void close() {
        queueLock.writeLock().lock();
        try {
            closed = true;
            try {
                connection.closeDispatcher(dispatcher);
                connection.close();
            } catch (InterruptedException ignored) {
                packetService.removeMessenger(this);
                Thread.currentThread().interrupt();
            }
            packetService.removeMessenger(this);
        } finally {
            queueLock.writeLock().unlock();
        }
    }

    @Override
    public boolean isClosed() { return closed || connection.getConnectedUrl() == null; }

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
        private final NATSMessagingService service;
        private final Options.Builder config = new Options.Builder();

        public Builder(@NotNull PacketService packetService, @NotNull String name, @NotNull String channelName, @NotNull UUID serverId, @NotNull MessagingHandler handler, long startupDelay, boolean dumpPackets, @NotNull File packetDirectory) {
            service = new NATSMessagingService(packetService, name, channelName, startupDelay, dumpPackets, packetDirectory);
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
        }

        public @NotNull Builder url(@NotNull String address, int port) {
            config.server("nats://" + address + ":" + port);
            return this;
        }

        public @NotNull Builder credentials(@NotNull String file) {
            config.authHandler(Nats.credentials(file));
            return this;
        }

        public @NotNull Builder life(int timeout) {
            config.connectionTimeout(Duration.ofMillis(timeout));
            return this;
        }

        public @NotNull NATSMessagingService build() throws IOException, InterruptedException {
            service.connection = Nats.connect(config.build());
            // Indefinite subscription
            if (service.startupDelay == 0L) {
                subscribe();
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
                    subscribe();
                    service.packetService.addMessenger(service);
                });
            }
            return service;
        }

        private void subscribe() {
            service.dispatcher = service.connection.createDispatcher(message -> {
                String subject = message.getSubject();
                try {
                    if (service.subjectName.equals(subject)) {
                        handleMessage(message.getData());
                    } else {
                        service.logger.warn("Got data from subject that should not exist: " + subject);
                    }
                } catch (IOException ex) {
                    service.logger.error("Could not handle message.", ex);
                }
            });
            service.dispatcher.subscribe(service.subjectName);
        }

        private void handleMessage(byte @NotNull [] body) throws IOException {
            ByteBuf b = alloc.buffer(body.length, body.length);
            ByteBuf data = null;
            try {
                b.writeBytes(body);
                data = service.decompressData(b);

                if (service.dumpPackets) {
                    service.dumpReceivedPacket(data);
                }

                UUID sender = new UUID(data.readLong(), data.readLong());
                if (service.serverId.equals(sender)) {
                    return;
                }

                byte packetVersion = CollectionProvider.getServerVersions().getOrDefault(sender, (byte) -1);
                if (packetVersion > -1 && packetVersion != service.packetService.getPacketVersion()) {
                    service.logger.warn("Server " + sender + " packet version " + String.format(
                            "0x%02X ",
                            packetVersion
                    ) + " does not match current packet version " + String.format("0x%02X ", service.packetService.getPacketVersion()) + ". Skipping packet.");
                    return;
                }

                UUID messageId = new UUID(data.readLong(), data.readLong());

                byte packetId = data.readByte();
                Packet packet;
                try {
                    packet = PacketManager.read(packetId, sender, data);
                    if (packet == null) {
                        service.logger.warn("Received packet ID that doesn't exist: " + packetId);
                        return;
                    }
                } catch (Exception ex) {
                    Class<? extends Packet> clazz = PacketManager.getPacket(packetId);
                    service.logger.error(
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
                    service.logger.warn("Server " + sender + " packet version is unknown, and packet type is of " + packet.getClass().getName() + ". Skipping packet.");
                    // There's a potential race condition here with double-sending a request, but it doesn't really matter
                    ByteBuf finalData = data;
                    CollectionProvider.getPacketProcessingQueue().compute(sender, (k, v) -> {
                        if (v == null) {
                            v = new CopyOnWriteArrayList<>();
                        }

                        if (v.isEmpty()) {
                            if (packet.verifyFullRead(finalData)) {
                                v.add(new Pair<>(messageId, packet));
                            }
                            service.packetService.queuePacket(new PacketVersionRequestPacket(sender, service.serverId));
                        } else {
                            if (packet.verifyFullRead(finalData)) {
                                v.add(new Pair<>(messageId, packet));
                            }
                        }

                        return v;
                    });
                    return;
                }

                if (packet.verifyFullRead(data)) {
                    service.handler.handlePacket(messageId, service.getName(), packet);
                }
            } finally {
                b.release();
                if (data != null) {
                    data.release();
                }
            }
        }
    }

    @Override
    public void sendPacket(@NotNull UUID messageId, @NotNull Packet packet) throws IOException {
        queueLock.readLock().lock();
        try {
            ByteBuf buffer = alloc.buffer(getInitialCapacity());
            try {
                buffer.writeBytes(serverIdBytes);
                buffer.writeLong(messageId.getMostSignificantBits());
                buffer.writeLong(messageId.getLeastSignificantBits());
                buffer.writeByte(PacketManager.getId(packet.getClass()));
                packet.write(buffer);
                addCapacity(buffer.writerIndex());

                if (dumpPackets) {
                    dumpSentPacket(buffer);
                }

                connection.publish(subjectName, compressData(buffer));
            } finally {
                buffer.release();
            }
        } catch (IllegalStateException ex) {
            throw new IOException(ex);
        } finally {
            queueLock.readLock().unlock();
        }
    }
}
