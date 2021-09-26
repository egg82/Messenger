package ninja.egg82.messenger.handler;

import ninja.egg82.messenger.core.Pair;
import ninja.egg82.messenger.packets.MultiPacket;
import ninja.egg82.messenger.packets.Packet;
import ninja.egg82.messenger.services.CollectionProvider;
import ninja.egg82.messenger.services.PacketService;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

public class MessagingHandlerImpl extends AbstractMessagingHandler implements MessagingHandler {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final List<AbstractMessagingHandler> handlers = new CopyOnWriteArrayList<>();

    public MessagingHandlerImpl(@NotNull PacketService packetService) {
        super(packetService);
    }

    public void addHandler(@NotNull AbstractMessagingHandler handler) { handlers.add(handler); }

    @Override
    public void handlePacket(@NotNull UUID messageId, @NotNull String fromService, @NotNull Packet packet) {
        if (CollectionProvider.isDuplicateMessage(messageId)) {
            return;
        }

        try {
            if (!handlePacket(packet)) {
                logger.warn("Did not handle packet: " + packet.getClass().getName());
            }
        } catch (Exception ex) {
            logger.error(ex.getClass().getName() + ": " + ex.getMessage(), ex);
        } finally {
            if (packetService.hasRedundancy()) {
                packetService.repeatPacket(messageId, packet, fromService); // Only repeat packets if we are redundant
            }
        }
    }

    @Override
    public void flushPacketBuffer(@NotNull UUID forServer) {
        CollectionProvider.getPacketProcessingQueue().computeIfPresent(forServer, (k, v) -> {
            for (Pair<@NotNull UUID, @NotNull Packet> p : v) {
                handlePacket(p.getT1(), "", p.getT2()); // Blank service
            }
            return null;
        });
    }

    @Override
    protected boolean handlePacket(@NotNull Packet packet) {
        if (packet instanceof MultiPacket) {
            handleMulti((MultiPacket) packet);
            return true;
        }

        for (AbstractMessagingHandler handler : handlers) {
            if (handler.handlePacket(packet)) {
                return true;
            }
        }

        return false;
    }

    private void handleMulti(@NotNull MultiPacket packet) {
        for (Packet p : packet.getPackets()) {
            if (!handlePacket(p)) {
                logger.warn("Did not handle packet: " + packet.getClass().getName());
            }
        }
    }
}
