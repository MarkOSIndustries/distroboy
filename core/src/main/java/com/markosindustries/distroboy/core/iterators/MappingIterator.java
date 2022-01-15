package com.markosindustries.distroboy.core.iterators;

import java.util.Iterator;
import java.util.function.Function;

/**
 * An iterator which will take elements in the wrapped iterator and apply a map operation to them
 *
 * @param <I> The type of elements in the input {@link Iterator}
 * @param <O> The type of elements in the output {@link Iterator}
 */
public class MappingIterator<I, O> implements Iterator<O> {
  private final Iterator<I> wrapped;
  private final Function<I, O> mapper;

  /**
   * Create a mapping iterator around an existing iterator. Note that the existing iterator must not
   * be used elsewhere once wrapped.
   *
   * @param wrapped The wrapped iterator to take items from
   * @param mapper The function to map wrapped iterator items to the output type
   */
  public MappingIterator(Iterator<I> wrapped, Function<I, O> mapper) {
    this.wrapped = wrapped;
    this.mapper = mapper;
  }

  @Override
  public boolean hasNext() {
    return wrapped.hasNext();
  }

  @Override
  public O next() {
    return mapper.apply(wrapped.next());
  }
}
