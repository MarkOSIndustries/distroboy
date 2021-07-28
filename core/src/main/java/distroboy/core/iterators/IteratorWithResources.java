package distroboy.core.iterators;

import java.util.Collections;
import java.util.Iterator;

public interface IteratorWithResources<I> extends Iterator<I>, AutoCloseable {
  static <I> IteratorWithResources<I> from(Iterator<I> iterator) {
    return from(
        iterator,
        () -> {
          /* NOOP on close() */
        });
  }

  static <I> IteratorWithResources<I> from(Iterator<I> iterator, AutoCloseable closeable) {
    return new IteratorWithResources<I>() {
      @Override
      public void close() throws Exception {
        closeable.close();
      }

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public I next() {
        return iterator.next();
      }
    };
  }

  static <I> IteratorWithResources<I> emptyIterator() {
    return from(Collections.emptyIterator());
  }
}
