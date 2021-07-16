package distroboy.core.operations;

import java.util.Iterator;

public interface FilterOp<I> extends ListOp<I, I> {
  boolean filter(I input);

  @Override
  default Iterator<I> apply(Iterator<I> input) {
    if (!input.hasNext()) {
      return input;
    }

    return new Iterator<I>() {
      private boolean hasNext = true;
      private I next = getNext();

      private I getNext() {
        while (input.hasNext()) {
          final var next = input.next();
          if (filter(next)) {
            return next;
          }
        }
        hasNext = false;
        return null;
      }

      @Override
      public boolean hasNext() {
        return hasNext;
      }

      @Override
      public I next() {
        final var toReturn = next;
        next = getNext();
        return toReturn;
      }
    };
  }
}
