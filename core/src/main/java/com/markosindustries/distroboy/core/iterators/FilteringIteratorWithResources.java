package com.markosindustries.distroboy.core.iterators;

import java.util.function.Predicate;

public class FilteringIteratorWithResources<I> implements IteratorWithResources<I> {
  private final IteratorWithResources<I> wrapped;
  private final Predicate<I> filter;
  private boolean hasNext;
  private I next;

  public FilteringIteratorWithResources(IteratorWithResources<I> wrapped, Predicate<I> filter) {
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
