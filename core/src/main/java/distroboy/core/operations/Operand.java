package distroboy.core.operations;

import distroboy.core.iterators.IteratorWithResources;
import distroboy.schemas.DataSourceRange;
import java.util.Iterator;
import java.util.List;

public interface Operand<I, CI> {
  default <O, CO> Operand<O, CO> then(Operation<I, O, CO> operation) {
    return new AppliedOperation<>(this, operation);
  }

  List<Operand<?, ?>> dependencies();

  IteratorWithResources<I> enumerateRangeForNode(DataSourceRange dataSourceRange) throws Exception;

  CI collect(Iterator<I> results);
}
