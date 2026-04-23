package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import java.util.Collections;
import java.util.List;

/**
 * A data source which came from an in memory list. <i>All cluster members <b>must</b> see the same
 * list in the same order</i>.
 *
 * @param <Input> The type of data in the {@link java.util.List}
 */
public class StaticDataSource<Input> implements DataSource<Input> {
  private final List<Input> data;

  /**
   * A data source which came from an in memory list. <i>All cluster members <b>must</b> see the
   * same list in the same order</i>.
   *
   * @param data The data to use as the source
   */
  public StaticDataSource(List<Input> data) {
    this.data = Collections.unmodifiableList(data);
  }

  @Override
  public long countOfFullSet() {
    return data.size();
  }

  @Override
  public IteratorWithResources<Input> enumerateRangeOfFullSet(
      long startInclusive, long endExclusive) {
    return IteratorWithResources.from(
        data.stream().skip(startInclusive).limit(endExclusive - startInclusive).iterator());
  }
}
