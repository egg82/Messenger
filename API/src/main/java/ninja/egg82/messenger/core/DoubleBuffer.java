package ninja.egg82.messenger.core;

import org.jetbrains.annotations.NotNull;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DoubleBuffer<T> {
    private final AtomicReference<Queue<T>> currentBuffer = new AtomicReference<>(new ConcurrentLinkedQueue<>());
    private final AtomicReference<Queue<T>> backBuffer = new AtomicReference<>(new ConcurrentLinkedQueue<>());
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public @NotNull Queue<T> getReadBuffer() {
        lock.readLock().lock();
        try {
            return backBuffer.get();
        } finally {
            lock.readLock().unlock();
        }
    }

    public @NotNull Queue<T> getWriteBuffer() {
        lock.readLock().lock();
        try {
            return currentBuffer.get();
        } finally {
            lock.readLock().unlock();
        }
    }

    public void swapBuffers() {
        lock.writeLock().lock();
        try {
            backBuffer.set(currentBuffer.getAndSet(backBuffer.get())); // currentBuffer.set(backBuffer.get()) -> backBuffer.set(oldCurrentBuffer)
        } finally {
            lock.writeLock().unlock();
        }
    }
}
