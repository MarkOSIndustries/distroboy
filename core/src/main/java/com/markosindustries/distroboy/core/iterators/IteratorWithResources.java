package com.markosindustries.distroboy.core.iterators;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * An {@link Iterator} which also has an {@link AutoCloseable} which should be {@link
 * AutoCloseable#close()}d when all items have been iterated.
 *
 * @param <T> The type of items in the iterator
 */
public interface IteratorWithResources<T> extends Iterator<T>, AutoCloseable {
  /** An AutoCloseable which does nothing */
  AutoCloseable NOOP_AUTOCLOSEABLE =
      () -> {
        /* NOOP on close() */
      };

  /**
   * Produce an {@link IteratorWithResources} which contains the provided elements
   *
   * @param <T> The type of elements
   * @param elements The elements to iterate
   * @return An {@link IteratorWithResources}
   */
  @SafeVarargs
  static <T> IteratorWithResources<T> of(T... elements) {
    return from(List.of(elements).iterator());
  }

  /**
   * Produce an {@link IteratorWithResources} which contains the provided elements, and will close
   * the provided closeable when {@link IteratorWithResources#close()} is called
   *
   * @param <T> The type of elements
   * @param closeable The resource to close when {@link IteratorWithResources#close()} is called
   * @param elements The elements to iterate
   * @return An {@link IteratorWithResources}
   */
  @SafeVarargs
  static <T> IteratorWithResources<T> of(AutoCloseable closeable, T... elements) {
    return from(List.of(elements).iterator(), closeable);
  }

  /**
   * Produce an {@link IteratorWithResources} which delegates to the given {@link Iterator}
   *
   * @param <T> The type of elements
   * @param iterator The iterator to delegate to
   * @return An {@link IteratorWithResources}
   */
  static <T> IteratorWithResources<T> from(Iterator<T> iterator) {
    if (iterator instanceof IteratorWithResources) {
      return (IteratorWithResources<T>) iterator;
    }
    if (iterator instanceof AutoCloseable) {
      return from(iterator, (AutoCloseable) iterator);
    }
    return from(iterator, NOOP_AUTOCLOSEABLE);
  }

  /**
   * Produce an {@link IteratorWithResources} which contains the elements in the given {@link
   * Iterable}
   *
   * @param <T> The type of elements
   * @param iterable The iterable to iterate
   * @return An {@link IteratorWithResources}
   */
  static <T> IteratorWithResources<T> from(Iterable<T> iterable) {
    if (iterable instanceof AutoCloseable) {
      return from(iterable.iterator(), (AutoCloseable) iterable);
    }
    return from(iterable.iterator(), NOOP_AUTOCLOSEABLE);
  }

  /**
   * Produce an {@link IteratorWithResources} which delegates to the given {@link Iterator}, and
   * will close the provided closeable when {@link IteratorWithResources#close()} is called
   *
   * @param <T> The type of elements
   * @param iterator The iterator to delegate to
   * @param closeable The resource to close when {@link IteratorWithResources#close()} is called
   * @return An {@link IteratorWithResources}
   */
  static <T> IteratorWithResources<T> from(Iterator<T> iterator, AutoCloseable closeable) {
    return new IteratorWithResources<T>() {
      @Override
      public void close() throws Exception {
        closeable.close();
      }

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public T next() {
        return iterator.next();
      }
    };
  }

  /**
   * Produce an empty {@link IteratorWithResources}
   *
   * @param <T> The type of elements
   * @return An empty {@link IteratorWithResources}
   */
  static <T> IteratorWithResources<T> emptyIterator() {
    return from(Collections.emptyIterator());
  }
}
