package com.markosindustries.distroboy.core.iterators;

import java.util.function.Function;

/**
 * An {@link IteratorWithResources} which will take elements in the wrapped iterator and apply a
 * flatmap operation to them
 *
 * @param <I> The type of elements in the input {@link IteratorWithResources}
 * @param <O> The type of elements in the output {@link IteratorWithResources}
 */
public class FlatMappingIteratorWithResources<I, O> implements IteratorWithResources<O> {
  private final IteratorWithResources<I> wrapped;
  private final Function<I, IteratorWithResources<O>> flatten;
  private IteratorWithResources<O> current = IteratorWithResources.emptyIterator();

  public FlatMappingIteratorWithResources(
      IteratorWithResources<I> wrapped, Function<I, IteratorWithResources<O>> flatten) {
    this.wrapped = wrapped;
    this.flatten = flatten;
  }

  private void ensureCurrentHasNext() {
    while (!current.hasNext() && wrapped.hasNext()) {
      try {
        current.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
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

  @Override
  public void close() throws Exception {
    wrapped.close();
  }
}
