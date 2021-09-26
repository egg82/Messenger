package ninja.egg82.messenger.handler;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import ninja.egg82.messenger.packets.Packet;
import ninja.egg82.messenger.packets.server.*;
import ninja.egg82.messenger.services.CollectionProvider;
import ninja.egg82.messenger.services.PacketService;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public abstract class AbstractServerMessagingHandler extends AbstractMessagingHandler {
    private final Cache<UUID, Long> aliveServers = Caffeine.newBuilder().expireAfterWrite(20L, TimeUnit.SECONDS)
            .evictionListener((RemovalListener<UUID, Long>) (uuid, timestamp, cause) -> {
                logger.warn("Server " + uuid + " has either shut down or timed out. Clearing its data.");
                if (uuid != null) {
                    handleShutdown(uuid);
                }
            }).build();

    private final UUID serverId;
    private final MessagingHandler messagingHandler;

    protected AbstractServerMessagingHandler(@NotNull UUID serverId, @NotNull PacketService packetService, @NotNull MessagingHandler messagingHandler) {
        super(packetService);
        this.serverId = serverId;
        this.messagingHandler = messagingHandler;
    }

    @Override
    protected boolean handlePacket(@NotNull Packet packet) {
        if (packet instanceof KeepAlivePacket) {
            handleKeepalive((KeepAlivePacket) packet);
            return true;
        } else if (packet instanceof InitializationPacket) {
            handleInitialization((InitializationPacket) packet);
            return true;
        } else if (packet instanceof PacketVersionPacket) {
            handlePacketVersion((PacketVersionPacket) packet);
            return true;
        } else if (packet instanceof PacketVersionRequestPacket) {
            handlePacketVersionRequest((PacketVersionRequestPacket) packet);
            return true;
        } else if (packet instanceof ShutdownPacket) {
            handleShutdown((ShutdownPacket) packet);
            return true;
        }

        return false;
    }

    protected void handleKeepalive(@NotNull KeepAlivePacket packet) {
        aliveServers.put(packet.getSender(), System.currentTimeMillis());
    }

    protected void handleInitialization(@NotNull InitializationPacket packet) {
        CollectionProvider.getServerVersions().put(packet.getServer(), packet.getPacketVersion());
        packetService.queuePackets(new PacketVersionPacket(packet.getServer(), serverId, packetService.getPacketVersion()));
    }

    protected void handlePacketVersion(@NotNull PacketVersionPacket packet) {
        if (!packet.getIntendedRecipient().equals(serverId)) {
            return;
        }

        CollectionProvider.getServerVersions().put(packet.getServer(), packet.getPacketVersion());

        messagingHandler.flushPacketBuffer(packet.getServer()); // Flush a server's received packet queue after we have the packet version for that server
    }

    protected void handlePacketVersionRequest(@NotNull PacketVersionRequestPacket packet) {
        if (!packet.getIntendedRecipient().equals(serverId)) {
            return;
        }

        packetService.queuePacket(new PacketVersionPacket(packet.getServer(), packet.getIntendedRecipient(), packetService.getPacketVersion()));
    }

    protected void handleShutdown(@NotNull ShutdownPacket packet) {
        aliveServers.invalidate(packet.getServer());
        handleShutdown(packet.getServer());
    }

    protected void handleShutdown(@NotNull UUID serverId) {
        CollectionProvider.getServerVersions().removeByte(serverId);
    }
}
