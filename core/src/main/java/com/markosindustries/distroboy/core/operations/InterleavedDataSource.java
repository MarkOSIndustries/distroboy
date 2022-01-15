package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.Cluster;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.iterators.SparseIteratorWithResources;

public abstract class InterleavedDataSource<I> implements DataSource<I> {
  private final int expectedClusterMembers;

  public InterleavedDataSource(Cluster cluster) {
    this.expectedClusterMembers = cluster.expectedClusterMembers;
  }

  @Override
  public final long countOfFullSet() {
    return expectedClusterMembers;
  }

  @Override
  public final IteratorWithResources<I> enumerateRangeOfFullSet(
      long startInclusive, long endExclusive) {
    return new SparseIteratorWithResources<I>(
        enumerateFullSet(), (int) startInclusive, expectedClusterMembers);
  }

  protected abstract IteratorWithResources<I> enumerateFullSet();
}
