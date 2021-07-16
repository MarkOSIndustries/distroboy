package distroboy.core.operations;

import java.util.Iterator;

@FunctionalInterface
public interface MapOp<I, O> extends ListOp<I, O> {
  O map(I input);

  @Override
  default Iterator<O> apply(Iterator<I> input) {
    return new Iterator<O>() {
      @Override
      public boolean hasNext() {
        return input.hasNext();
      }

      @Override
      public O next() {
        return map(input.next());
      }
    };
  }
}
