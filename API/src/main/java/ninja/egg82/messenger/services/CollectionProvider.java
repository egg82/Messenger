package ninja.egg82.messenger.services;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import it.unimi.dsi.fastutil.objects.Object2ByteArrayMap;
import it.unimi.dsi.fastutil.objects.Object2ByteMap;
import ninja.egg82.messenger.core.Pair;
import ninja.egg82.messenger.packets.Packet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class CollectionProvider {
    private CollectionProvider() { }

    private static final Object2ByteMap<UUID> serverVersions = new Object2ByteArrayMap<>();

    public static Object2ByteMap<UUID> getServerVersions() { return serverVersions; }

    private static final @NotNull Cache<UUID, @Nullable Boolean> messageCache = Caffeine.newBuilder()
            .expireAfterWrite(2L, TimeUnit.MINUTES)
            .expireAfterAccess(30L, TimeUnit.SECONDS)
            .build();

    public static @NotNull Cache<UUID, @Nullable Boolean> getMessageCache() { return messageCache; }

    public static boolean isDuplicateMessage(@NotNull UUID messageId) {
        AtomicBoolean retVal = new AtomicBoolean(true);
        messageCache.get(messageId, k -> {
            retVal.set(false);
            return Boolean.FALSE;
        });
        return retVal.get();
    }

    private static final ConcurrentMap<UUID, @Nullable List<@NotNull Pair<@NotNull UUID, @NotNull Packet>>> packetProcessingQueue = new ConcurrentHashMap<>();

    public static @NotNull ConcurrentMap<UUID, @Nullable List<@NotNull Pair<@NotNull UUID, @NotNull Packet>>> getPacketProcessingQueue() { return packetProcessingQueue; }
}
