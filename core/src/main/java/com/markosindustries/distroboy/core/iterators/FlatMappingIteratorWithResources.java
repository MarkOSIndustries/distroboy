package com.markosindustries.distroboy.core.iterators;

import java.util.Objects;
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

  /**
   * Create a flatmapping iterator around an existing iterator. Note that the existing iterator must
   * not be used elsewhere once wrapped.
   *
   * @param wrapped The wrapped iterator to take items from
   * @param flatten The function to extract iterators from each element in the wrapped iterator
   */
  public FlatMappingIteratorWithResources(
      IteratorWithResources<I> wrapped, Function<I, IteratorWithResources<O>> flatten) {
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
      try {
        current.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      current = flatten.apply(wrapped.next());
    }
    if (!current.hasNext()) {
      try {
        current.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
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
