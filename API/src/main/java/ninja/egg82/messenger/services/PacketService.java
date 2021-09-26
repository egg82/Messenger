package ninja.egg82.messenger.services;

import ninja.egg82.messenger.MessagingService;
import ninja.egg82.messenger.core.DoubleBuffer;
import ninja.egg82.messenger.packets.MultiPacket;
import ninja.egg82.messenger.packets.Packet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PacketService {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final ExecutorService workPool;
    private final boolean redundancy;
    private final byte packetVersion;

    public PacketService(int poolSize, boolean redundancy, byte packetVersion) {
        workPool = Executors.newFixedThreadPool(Math.max(poolSize, 1));
        this.redundancy = redundancy;
        this.packetVersion = packetVersion;
    }

    public boolean hasRedundancy() { return redundancy; }

    public byte getPacketVersion() { return packetVersion; }

    public void shutdown() {
        workPool.shutdown();
        try {
            if (!workPool.awaitTermination(4L, TimeUnit.SECONDS)) {
                workPool.shutdownNow();
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    private final List<MessagingService> messengers = new CopyOnWriteArrayList<>(); // Reads > writes
    private final ReadWriteLock servicesLock = new ReentrantReadWriteLock();

    public boolean addMessenger(@NotNull MessagingService service) {
        servicesLock.writeLock().lock();
        try {
            for (MessagingService m : messengers) {
                if (m.getName().equals(service.getName())) {
                    return false;
                }
            }
            messengers.add(service);
            return true;
        } finally {
            servicesLock.writeLock().unlock();
        }
    }

    public boolean removeMessenger(@NotNull MessagingService service) { return removeMessenger(service.getName()); }

    public boolean removeMessenger(@NotNull String serviceName) {
        servicesLock.writeLock().lock();
        try {
            for (Iterator<MessagingService> i = messengers.iterator(); i.hasNext();) {
                MessagingService m = i.next();
                if (m.getName().equals(serviceName)) {
                    i.remove();
                    return true;
                }
            }
            return false;
        } finally {
            servicesLock.writeLock().unlock();
        }
    }

    private final DoubleBuffer<Packet> packetQueue = new DoubleBuffer<>();
    private final AtomicBoolean requiresSending = new AtomicBoolean(false);
    private final AtomicInteger currentIndex = new AtomicInteger(-1);

    public void queuePackets(@NotNull Collection<@NotNull Packet> packets) {
        packetQueue.getWriteBuffer().addAll(packets);
        requiresSending.set(true);
    }

    public void queuePackets(@NotNull Packet @NotNull ... packets) {
        for (Packet packet : packets) {
            packetQueue.getWriteBuffer().add(packet);
        }
        requiresSending.set(true);
    }

    public void queuePacket(@NotNull Packet packet) {
        packetQueue.getWriteBuffer().add(packet);
        requiresSending.set(true);
    }

    public void repeatPacket(@NotNull UUID messageId, @NotNull Packet packet, @NotNull String fromServiceName) { sendPacket(messageId, packet, fromServiceName); }

    public boolean flushQueue() {
        if (!requiresSending.compareAndSet(true, false)) {
            return false;
        }

        packetQueue.swapBuffers();

        UUID messageId = UUID.randomUUID();
        CollectionProvider.getMessageCache().put(messageId, Boolean.TRUE);

        if (packetQueue.getReadBuffer().size() > 1) {
            MultiPacket multi = new MultiPacket();
            Packet packet;
            while ((packet = packetQueue.getReadBuffer().poll()) != null) {
                if (!multi.getPackets().add(packet)) {
                    logger.warn("Skipping duplicate packet " + packet.getClass().getSimpleName());
                }
            }

            if (!multi.getPackets().isEmpty()) {
                sendPacket(messageId, multi, null);
            }
        } else {
            Packet packet = packetQueue.getReadBuffer().poll();
            if (packet != null) {
                sendPacket(messageId, packet, null);
            }
        }

        return true;
    }

    private void sendPacket(@NotNull UUID messageId, @NotNull Packet packet, @Nullable String fromServiceName) {
        if (redundancy) {
            servicesLock.readLock().lock();
            try {
                for (MessagingService messenger : messengers) {
                    if (messenger.getName().equals(fromServiceName)) {
                        continue;
                    }

                    workPool.execute(() -> {
                        try {
                            messenger.sendPacket(messageId, packet);
                        } catch (IOException | TimeoutException ex) {
                            logger.warn("Could not broadcast packet " + packet.getClass().getSimpleName() + " through " + messenger.getName(), ex);
                        }
                    });
                }
            } finally {
                servicesLock.readLock().unlock();
            }
        } else {
            if (fromServiceName != null) {
                return;
            }

            servicesLock.readLock().lock();
            try {
                int index = getNextService();
                int initialIndex = index;
                boolean sent = false;

                do {
                    MessagingService messenger = messengers.get(index);

                    try {
                        messenger.sendPacket(messageId, packet);
                        sent = true;
                        break;
                    } catch (IOException | TimeoutException ex) {
                        logger.warn("Could not broadcast packet " + packet.getClass().getSimpleName() + " through " + messenger.getName(), ex);
                        index = getNextService();
                    }
                } while (index != initialIndex); // This will be true if we've run through all of our services and wrapped around to the start again

                if (!sent) {
                    logger.error("Could not broadcast packet " + packet.getClass().getSimpleName() + " through any available messaging service.");
                }
            } finally {
                servicesLock.readLock().unlock();
            }
        }
    }

    private int getNextService() {
        return currentIndex.updateAndGet(v -> {
            if (v >= messengers.size() - 1) {
                return 0;
            }
            return v + 1;
        });
    }
}
