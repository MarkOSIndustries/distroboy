package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.IteratorTo;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.schemas.DataSourceRange;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Represents a place data can be loaded from for a {@link DistributedOpSequence}. The only
 * requirements are that
 *
 * <ul>
 *   <li>each node in the cluster will see the same data set
 *   <li>we know how many items are in the data set
 *   <li>we can load a requested index range of the data
 * </ul>
 *
 * @param <I>
 */
public interface DataSource<I> extends Operand<I, List<I>> {
  long countOfFullSet();

  IteratorWithResources<I> enumerateRangeOfFullSet(
      final long startInclusive, final long endExclusive);

  @Override
  default List<Operand<?, ?>> dependencies() {
    return Collections.emptyList();
  }

  default IteratorWithResources<I> enumerateRangeForNode(final DataSourceRange dataSourceRange) {
    return enumerateRangeOfFullSet(
        dataSourceRange.getStartInclusive(), dataSourceRange.getEndExclusive());
  }

  @Override
  default List<I> collect(final Iterator<I> results) {
    return IteratorTo.list(results);
  }
}
