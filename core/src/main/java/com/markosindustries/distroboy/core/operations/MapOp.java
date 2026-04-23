package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.iterators.MappingIteratorWithResources;

/**
 * A high level interface for any operation which takes a distributed data set and transforms each
 * item in it via the given {@link #map} function
 *
 * @param <Input> The type of the input data set items
 * @param <Output> The type of the output data set items
 */
@FunctionalInterface
public interface MapOp<Input, Output> extends ListOp<Input, Output> {
  /**
   * Transform an input to the output type
   *
   * @param input The value to be mapped
   * @return The result of the map operation
   */
  Output map(Input input);

  @Override
  default IteratorWithResources<Output> apply(IteratorWithResources<Input> input) throws Exception {
    return new MappingIteratorWithResources<>(input, this::map);
  }
}
