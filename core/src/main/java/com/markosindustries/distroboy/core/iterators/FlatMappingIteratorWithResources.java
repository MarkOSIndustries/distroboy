package com.markosindustries.distroboy.core.iterators;

import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

/**
 * An {@link IteratorWithResources} which will take elements in the wrapped iterator and apply a
 * flatmap operation to them. If the wrapped iterator or any of its child iterators are
 * AutoCloseable (such as {@link IteratorWithResources}) they will be closed appropriately too.
 *
 * @param <I> The type of elements in the input {@link Iterator}
 * @param <O> The type of elements in the output {@link IteratorWithResources}
 */
public class FlatMappingIteratorWithResources<I, O> implements IteratorWithResources<O> {
  private final Iterator<I> wrapped;
  private final Function<I, ? extends Iterator<O>> flatten;
  private Iterator<O> currentIterator = Collections.emptyIterator();
  private AutoCloseable currentCloseable = NOOP_AUTOCLOSEABLE;

  /**
   * Create a flatmapping iterator around an existing iterator. Note that the existing iterator must
   * not be used elsewhere once wrapped.
   *
   * @param wrapped The wrapped iterator to take items from
   * @param flatten The function to extract iterators from each element in the wrapped iterator
   */
  public FlatMappingIteratorWithResources(
      Iterator<I> wrapped, Function<I, ? extends Iterator<O>> flatten) {
    if (Objects.isNull(wrapped)) {
      throw new IllegalArgumentException("Wrapped iterator cannot be null");
    }
    if (Objects.isNull(flatten)) {
      throw new IllegalArgumentException("Flatten cannot be null");
    }
    this.wrapped = wrapped;
    this.flatten = flatten;
  }

  private void ensureCurrentHasNext() {
    while (!currentIterator.hasNext() && wrapped.hasNext()) {
      try {
        currentCloseable.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      currentIterator = flatten.apply(wrapped.next());
      currentCloseable =
          (currentIterator instanceof AutoCloseable)
              ? (AutoCloseable) currentIterator
              : NOOP_AUTOCLOSEABLE;
    }
    if (!currentIterator.hasNext()) {
      try {
        currentCloseable.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public boolean hasNext() {
    ensureCurrentHasNext();
    return currentIterator.hasNext();
  }

  @Override
  public O next() {
    ensureCurrentHasNext();
    return currentIterator.next();
  }

  @Override
  public void close() throws Exception {
    if (wrapped instanceof AutoCloseable) {
      ((AutoCloseable) wrapped).close();
    }
  }
}
