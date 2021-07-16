package distroboy.core.operations;

import distroboy.schemas.DataSourceRange;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public interface DataSource<I> extends Operand<I, List<I>> {
  long countOfFullSet();

  Iterator<I> enumerateRangeOfFullSet(final long startInclusive, final long endExclusive);

  @Override
  default List<Operand<?, ?>> dependencies() {
    return Collections.emptyList();
  }

  default Iterator<I> enumerateRangeForNode(final DataSourceRange dataSourceRange) {
    return enumerateRangeOfFullSet(
        dataSourceRange.getStartInclusive(), dataSourceRange.getEndExclusive());
  }

  @Override
  default List<I> collect(final Iterator<I> results) {
    return ListFrom.iterator(results);
  }
}
