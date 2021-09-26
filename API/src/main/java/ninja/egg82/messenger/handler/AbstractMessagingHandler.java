package ninja.egg82.messenger.handler;

import ninja.egg82.messenger.packets.Packet;
import ninja.egg82.messenger.services.PacketService;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMessagingHandler {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected final PacketService packetService;

    protected AbstractMessagingHandler(@NotNull PacketService packetService) { this.packetService = packetService; }

    protected abstract boolean handlePacket(@NotNull Packet packet);
}
