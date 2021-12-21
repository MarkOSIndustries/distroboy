package com.markosindustries.distroboy.core.iterators;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * An {@link Iterator} which also has an {@link AutoCloseable} which should be {@link
 * AutoCloseable#close()}d when all items have been iterated.
 *
 * @param <I> The type of items in the iterator
 */
public interface IteratorWithResources<I> extends Iterator<I>, AutoCloseable {
  static <I> IteratorWithResources<I> of(I element) {
    return from(List.of(element).iterator());
  }

  static <I> IteratorWithResources<I> from(Iterator<I> iterator) {
    return from(
        iterator,
        () -> {
          /* NOOP on close() */
        });
  }

  static <I> IteratorWithResources<I> from(Iterable<I> iterable) {
    return from(
        iterable.iterator(),
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
