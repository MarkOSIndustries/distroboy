package com.markosindustries.distroboy.core.iterators;

import java.util.Collections;
import java.util.Iterator;
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

  public FlatMappingIterator(Iterator<I> wrapped, Function<I, Iterator<O>> flatten) {
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
