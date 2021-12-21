package com.markosindustries.distroboy.core.iterators;

import java.util.Iterator;
import java.util.function.Predicate;

public class FilteringIterator<I> implements Iterator<I> {
  private final Iterator<I> wrapped;
  private final Predicate<I> filter;
  private boolean hasNext;
  private I next;

  public FilteringIterator(Iterator<I> wrapped, Predicate<I> filter) {
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
}
