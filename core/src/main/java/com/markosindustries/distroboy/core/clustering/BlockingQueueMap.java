package com.markosindustries.distroboy.core.clustering;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class BlockingQueueMap<K, V> {
  private final Map<K, BlockingQueue<V>> queueMap = new HashMap<>();

  public synchronized void supplyValue(K key, V value) {
    final var queue = queueMap.computeIfAbsent(key, unused -> new LinkedBlockingQueue<>());
    queue.add(value);
    this.notify();
  }

  public synchronized V awaitPeekValue(K key, AtomicBoolean disbanding) {
    final var queue = queueMap.computeIfAbsent(key, unused -> new LinkedBlockingQueue<>());
    V value = queue.peek();
    while (value == null) {
      try {
        if (disbanding.get()) {
          throw new RuntimeException();
        }
        this.wait(100);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      value = queue.peek();
    }
    return value;
  }

  public synchronized V awaitPollValue(K key, AtomicBoolean disbanding) {
    final var queue = queueMap.computeIfAbsent(key, unused -> new LinkedBlockingQueue<>());
    while (queue.peek() == null) {
      try {
        if (disbanding.get()) {
          throw new RuntimeException();
        }
        this.wait(100);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return queue.poll();
  }
}
