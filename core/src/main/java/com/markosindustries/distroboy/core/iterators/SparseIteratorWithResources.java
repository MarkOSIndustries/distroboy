package com.markosindustries.distroboy.core.iterators;

import java.util.Iterator;
import java.util.Objects;

/**
 * An {@link IteratorWithResources} which takes every nth element from the wrapped {@link Iterator},
 * starting at some offset
 *
 * @param <I> The type of elements in the {@link Iterator}
 */
public class SparseIteratorWithResources<I> implements IteratorWithResources<I> {
  private final IteratorWithResources<I> wrapped;
  private final int interval;
  private boolean hasNext;
  private I next;

  /**
   * Create a sparse iterator around an existing iterator. Note that the existing iterator must not
   * be used elsewhere once wrapped.
   *
   * @param wrapped The wrapped iterator to take items from
   * @param startingOffset The number of items to skip at the start. Must not be negative.
   * @param interval The number of items to advance after each output. Must be 1 or more.
   */
  public SparseIteratorWithResources(
      IteratorWithResources<I> wrapped, int startingOffset, int interval) {
    if (Objects.isNull(wrapped)) {
      throw new IllegalArgumentException("Wrapped iterator cannot be null");
    }
    if (startingOffset < 0) {
      throw new IllegalArgumentException("Starting offset must be non-negative");
    }
    if (interval < 1) {
      throw new IllegalArgumentException("Interval must be greater than zero");
    }
    this.wrapped = wrapped;
    this.interval = interval;
    this.hasNext = true;
    this.next = getNext(startingOffset + 1);
  }

  private I getNext(final int advanceBy) {
    int remainingSkips = advanceBy;
    I next = null;
    while (remainingSkips > 0) {
      if (wrapped.hasNext()) {
        next = wrapped.next();
        remainingSkips--;
      } else {
        hasNext = false;
        return null;
      }
    }
    return next;
  }

  @Override
  public boolean hasNext() {
    return hasNext;
  }

  @Override
  public I next() {
    final var toReturn = next;
    next = getNext(interval);
    return toReturn;
  }

  @Override
  public void close() throws Exception {
    wrapped.close();
  }
}
