package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import java.util.Collection;

/**
 * A data source which came from an in memory collection. Typically this is for internal use by
 * distroboy when doing dataset redistributions (eg: hashing and grouping). <br>
 * Care must be taken to ensure that all cluster nodes will arrive at a consistent conclusion as to
 * what the distributed data set contains. <br>
 * eg: If you run a job with 4 nodes who all declare the same collection - you will have 4 copies of
 * each entry in your distributed data set.
 *
 * @param <I> The type of data in the {@link java.util.Collection}
 */
public class StaticDataSource<I> implements DataSource<I> {
  private final Collection<I> data;

  public StaticDataSource(Collection<I> data) {
    this.data = data;
  }

  @Override
  public long countOfFullSet() {
    return data.size();
  }

  @Override
  public IteratorWithResources<I> enumerateRangeOfFullSet(long startInclusive, long endExclusive) {
    return IteratorWithResources.from(
        data.stream().skip(startInclusive).limit(endExclusive).iterator());
  }
}
