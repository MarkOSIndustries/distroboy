package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.Cluster;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.iterators.SparseIteratorWithResources;

/**
 * A data source which works by iterating the data set from all nodes, with each node taking every
 * Nth item (where N is the number of cluster members). This can be more efficient when determining
 * the size of the data set is costly.
 *
 * @param <I> The type of items in the data source
 */
public abstract class InterleavedDataSource<I> implements DataSource<I> {
  private final int expectedClusterMembers;

  /**
   * A data source which works by iterating the data set from all nodes, with each node taking every
   * Nth item (where N is the number of cluster members). This can be more efficient when
   * determining the size of the data set is costly.
   *
   * @param cluster The {@link Cluster} which will be retrieving the data
   */
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

  /**
   * Produce an iterator over the entire dataset, without need of seeking to a certain range. The
   * appropriate seeking operations will be done by each node to ensure a mutually exclusive access
   * pattern.
   *
   * @return An {@link IteratorWithResources} over the entire dataset
   */
  protected abstract IteratorWithResources<I> enumerateFullSet();
}
