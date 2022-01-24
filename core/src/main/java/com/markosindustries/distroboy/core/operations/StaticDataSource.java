package com.markosindustries.distroboy.core.operations;

import static java.util.Collections.unmodifiableCollection;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import java.util.Collection;

/**
 * A data source which came from an in memory collection. Typically, this is for internal use by
 * distroboy when doing dataset redistributions (eg: hashing and grouping).
 *
 * @param <I> The type of data in the {@link java.util.Collection}
 */
public class StaticDataSource<I> implements DataSource<I> {
  private final Collection<I> data;

  /**
   * A data source which came from an in memory collection. Typically, this is for internal use by
   * distroboy when doing dataset redistributions (eg: hashing and grouping).
   *
   * @param data The data to use as the source
   */
  public StaticDataSource(Collection<I> data) {
    this.data = unmodifiableCollection(data);
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
