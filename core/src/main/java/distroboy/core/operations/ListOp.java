package distroboy.core.operations;

import java.util.Iterator;
import java.util.List;

public interface ListOp<I, O> extends Operation<I, O, List<O>> {
  @Override
  default List<O> collect(Iterator<O> results) {
    return ListFrom.iterator(results);
  }
}
