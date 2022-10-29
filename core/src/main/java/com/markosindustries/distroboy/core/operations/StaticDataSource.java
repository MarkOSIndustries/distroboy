package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import java.util.Collections;
import java.util.List;

/**
 * A data source which came from an in memory list. <i>All cluster members <b>must</b> see the same
 * list in the same order</i>.
 *
 * @param <I> The type of data in the {@link java.util.Collection}
 */
public class StaticDataSource<I> implements DataSource<I> {
  private final List<I> data;

  /**
   * A data source which came from an in memory list. <i>All cluster members <b>must</b> see the
   * same list in the same order</i>.
   *
   * @param data The data to use as the source
   */
  public StaticDataSource(List<I> data) {
    this.data = Collections.unmodifiableList(data);
  }

  @Override
  public long countOfFullSet() {
    return data.size();
  }

  @Override
  public IteratorWithResources<I> enumerateRangeOfFullSet(long startInclusive, long endExclusive) {
    return IteratorWithResources.from(
        data.stream().skip(startInclusive).limit(endExclusive - startInclusive).iterator());
  }
}
