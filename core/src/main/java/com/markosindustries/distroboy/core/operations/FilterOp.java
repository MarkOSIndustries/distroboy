package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.FilteringIteratorWithResources;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;

/**
 * A high level interface for any operation which takes a distributed data set and keeps only the
 * items for which the given {@link #filter} function returns {@code true}.
 *
 * @param <I> The type of the data set items
 */
public interface FilterOp<I> extends ListOp<I, I> {
  boolean filter(I input);

  @Override
  default IteratorWithResources<I> apply(IteratorWithResources<I> input) {
    if (!input.hasNext()) {
      return input;
    }

    return new FilteringIteratorWithResources<>(input, this::filter);
  }
}
