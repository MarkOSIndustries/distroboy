package com.markosindustries.distroboy.core.iterators;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * An {@link IteratorWithResources} which will take elements in the wrapped iterator and filter them
 *
 * @param <I> The type of elements in {@link IteratorWithResources}
 */
public class FilteringIteratorWithResources<I> implements IteratorWithResources<I> {
  private final IteratorWithResources<I> wrapped;
  private final Predicate<I> filter;
  private boolean hasNext;
  private I next;

  /**
   * Create a filtering iterator around an existing iterator. Note that the existing iterator must
   * not be used elsewhere once wrapped.
   *
   * @param wrapped The wrapped iterator to take items from
   * @param filter The condition under which items are returned from this iterator
   */
  public FilteringIteratorWithResources(IteratorWithResources<I> wrapped, Predicate<I> filter) {
    if (Objects.isNull(wrapped)) {
      throw new IllegalArgumentException("Wrapped iterator cannot be null");
    }
    if (Objects.isNull(filter)) {
      throw new IllegalArgumentException("Filter cannot be null");
    }
    this.wrapped = wrapped;
    this.filter = filter;
    this.hasNext = true;
    this.next = getNext();
  }

  private I getNext() {
    while (wrapped.hasNext()) {
      final var next = wrapped.next();
      if (filter.test(next)) {
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

  @Override
  public void close() throws Exception {
    wrapped.close();
  }
}
