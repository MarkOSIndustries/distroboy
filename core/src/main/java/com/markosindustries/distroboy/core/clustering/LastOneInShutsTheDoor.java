package com.markosindustries.distroboy.core.clustering;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public interface LastOneInShutsTheDoor {
  interface Party<M> {
    boolean enter(M member);

    default Object getLock() {
      return this;
    }
  }

  static <K, M, P extends LastOneInShutsTheDoor.Party<M>> Optional<P> join(
      BlockingQueueMap<K, P> blockingQueueMap, K key, M member, AtomicBoolean disbanding) {
    final P party = blockingQueueMap.awaitPeekValue(key, disbanding);
    synchronized (party.getLock()) {
      final var isLastOne = party.enter(member);
      if (isLastOne) {
        return Optional.of(blockingQueueMap.awaitPollValue(key, disbanding));
      }
    }
    return Optional.empty();
  }
}
