package ninja.egg82.messenger.packets;

import io.netty.buffer.ByteBuf;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.UUID;

public interface Packet extends Serializable {
    void read(@NotNull ByteBuf buffer);

    void write(@NotNull ByteBuf buffer);

    @NotNull UUID getSender();

    boolean verifyFullRead(@NotNull ByteBuf buffer);
}
