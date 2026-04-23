package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.FilteringIteratorWithResources;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;

/**
 * A high level interface for any operation which takes a distributed data set and keeps only the
 * items for which the given {@link #filter} function returns {@code true}.
 *
 * @param <Input> The type of the data set items
 */
public interface FilterOp<Input> extends ListOp<Input, Input> {
  /**
   * Determine whether a given value should be passed through to the output of this operation
   *
   * @param input The value to decide whether to include or omit from output
   * @return <code>true</code> if the value should be included, or <code>false</code> if it should
   *     be omitted
   */
  boolean filter(Input input);

  @Override
  default IteratorWithResources<Input> apply(IteratorWithResources<Input> input) {
    if (!input.hasNext()) {
      return input;
    }

    return new FilteringIteratorWithResources<>(input, this::filter);
  }
}
