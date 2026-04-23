package com.markosindustries.distroboy.core.iterators;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * An iterator which will take elements in the wrapped iterator and filter them
 *
 * @param <T> The type of elements in {@link Iterator}
 */
public class FilteringIterator<T> implements Iterator<T> {
  private final Iterator<T> wrapped;
  private final Predicate<T> filter;
  private boolean hasNext;
  private T next;

  /**
   * Create a filtering iterator around an existing iterator. Note that the existing iterator must
   * not be used elsewhere once wrapped.
   *
   * @param wrapped The wrapped iterator to take items from
   * @param filter The condition under which items are returned from this iterator
   */
  public FilteringIterator(Iterator<T> wrapped, Predicate<T> filter) {
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

  private T getNext() {
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
  public T next() {
    final var toReturn = next;
    next = getNext();
    return toReturn;
  }
}
