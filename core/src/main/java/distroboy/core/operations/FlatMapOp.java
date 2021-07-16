package distroboy.core.operations;

import java.util.Iterator;

public interface FlatMapOp<I, O> extends ListOp<I, O> {
  Iterator<O> flatMap(I input);

  @Override
  default Iterator<O> apply(Iterator<I> input) {
    return new FlatMapIterator<>(input, this::flatMap);
  }
}
