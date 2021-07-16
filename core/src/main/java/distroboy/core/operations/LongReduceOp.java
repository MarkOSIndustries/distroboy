package distroboy.core.operations;

import java.util.Iterator;
import java.util.List;

public interface LongReduceOp<I> extends ReduceOp<I, Long> {
  default Long initAggregate() {
    return 0L;
  }

  default Iterator<Long> asIterator(Long aggregate) {
    return List.of(aggregate).iterator();
  }
}
