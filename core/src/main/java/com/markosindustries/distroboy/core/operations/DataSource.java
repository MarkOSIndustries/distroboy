package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.IteratorTo;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.schemas.DataSourceRange;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

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
