package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import java.util.List;

public interface LongReduceOp<I> extends ReduceOp<I, Long> {
  default Long initAggregate() {
    return 0L;
  }

  default IteratorWithResources<Long> asIterator(Long aggregate) {
    return IteratorWithResources.from(List.of(aggregate).iterator());
  }
}
