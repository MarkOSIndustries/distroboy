package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.iterators.MappingIteratorWithResources;

/**
 * A high level interface for any operation which takes a distributed data set and transforms each
 * item in it via the given {@link #map} function
 *
 * @param <I> The type of the input data set items
 * @param <O> The type of the output data set items
 */
@FunctionalInterface
public interface MapOp<I, O> extends ListOp<I, O> {
  O map(I input);

  @Override
  default IteratorWithResources<O> apply(IteratorWithResources<I> input) throws Exception {
    return new MappingIteratorWithResources<>(input, this::map);
  }
}
