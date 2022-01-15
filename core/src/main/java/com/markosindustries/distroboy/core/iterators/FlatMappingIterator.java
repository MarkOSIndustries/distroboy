package com.markosindustries.distroboy.core.iterators;

import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

/**
 * An iterator which will take elements in the wrapped iterator and apply a flatmap operation to
 * them
 *
 * @param <I> The type of elements in the input {@link Iterator}
 * @param <O> The type of elements in the output {@link Iterator}
 */
public class FlatMappingIterator<I, O> implements Iterator<O> {
  private final Iterator<I> wrapped;
  private final Function<I, Iterator<O>> flatten;
  private Iterator<O> current = Collections.emptyIterator();

  /**
   * Create a flatmapping iterator around an existing iterator. Note that the existing iterator must
   * not be used elsewhere once wrapped.
   *
   * @param wrapped The wrapped iterator to take items from
   * @param flatten The function to extract iterators from each element in the wrapped iterator
   */
  public FlatMappingIterator(Iterator<I> wrapped, Function<I, Iterator<O>> flatten) {
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
    while (!current.hasNext() && wrapped.hasNext()) {
      current = flatten.apply(wrapped.next());
    }
  }

  @Override
  public boolean hasNext() {
    ensureCurrentHasNext();
    return current.hasNext();
  }

  @Override
  public O next() {
    ensureCurrentHasNext();
    return current.next();
  }
}
