package ninja.egg82.messenger.handler;

import ninja.egg82.messenger.packets.Packet;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;

public interface MessagingHandler {
    void handlePacket(@NotNull UUID messageId, @NotNull String fromService, @NotNull Packet packet);

    void flushPacketBuffer(@NotNull UUID forServer);
}
