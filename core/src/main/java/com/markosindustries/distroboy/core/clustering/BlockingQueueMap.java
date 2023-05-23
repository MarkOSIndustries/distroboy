package com.markosindustries.distroboy.core.clustering;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class BlockingQueueMap<K, V> {
  private final Map<K, BlockingQueue<V>> queueMap = new HashMap<>();

  public synchronized void supplyValue(K key, V value) {
    final var queue = queueMap.computeIfAbsent(key, unused -> new LinkedBlockingQueue<>());
    queue.add(value);
    this.notify();
  }

  public synchronized V awaitPeekValue(K key) {
    final var queue = queueMap.computeIfAbsent(key, unused -> new LinkedBlockingQueue<>());
    V value = queue.peek();
    while (value == null) {
      try {
        this.wait();
      } catch (InterruptedException e) {
        // Just try again
      }
      value = queue.peek();
    }
    return value;
  }

  public synchronized V awaitPollValue(K key) {
    final var queue = queueMap.computeIfAbsent(key, unused -> new LinkedBlockingQueue<>());
    while (queue.peek() == null) {
      try {
        this.wait();
      } catch (InterruptedException e) {
        // Just try again
      }
    }
    return queue.poll();
  }
}
