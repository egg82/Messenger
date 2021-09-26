package ninja.egg82.messenger;

import io.netty.buffer.ByteBuf;
import ninja.egg82.messenger.packets.Packet;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;

public interface PacketSupplier<T extends Packet> {
    @NotNull T create(@NotNull UUID sender, @NotNull ByteBuf buffer);
}
