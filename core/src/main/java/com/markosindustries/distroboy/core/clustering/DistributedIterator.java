package com.markosindustries.distroboy.core.clustering;

import com.markosindustries.distroboy.core.Cluster;
import com.markosindustries.distroboy.core.clustering.serialisation.Serialiser;
import com.markosindustries.distroboy.core.clustering.serialisation.Serialisers;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.Supplier;

public class DistributedIterator<T> implements Iterator<T> {
  private final Iterator<T> items;
  private final Cluster cluster;
  private final Serialiser<T> serialiser;
  private Boolean hasNext;

  public DistributedIterator(
      final Cluster cluster,
      Supplier<? extends Iterable<T>> supplyValuesOnLeader,
      Serialiser<T> serialiser) {
    this.cluster = cluster;
    this.serialiser = serialiser;

    this.cluster.waitForAllMembers();
    this.items =
        cluster.isLeader() ? supplyValuesOnLeader.get().iterator() : Collections.emptyIterator();
    this.hasNext = cluster.waitAndReplicateToAllMembers(items::hasNext, Serialisers.booleanValues);
  }

  @Override
  public boolean hasNext() {
    return hasNext;
  }

  @Override
  public T next() {
    final var next = cluster.waitAndReplicateToAllMembers(items::next, serialiser);
    this.hasNext = cluster.waitAndReplicateToAllMembers(items::hasNext, Serialisers.booleanValues);
    return next;
  }
}
