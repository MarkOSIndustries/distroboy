package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.FlatMappingIteratorWithResources;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import java.util.Iterator;

/**
 * A high level interface for any operation which takes a distributed data set and transforms each
 * item in it via the given {@link #flatMap} function into an {@link Iterator}. The resulting
 * iterators will be flattened into one large {@link Iterator}.
 *
 * @param <I> The type of the input data set items
 * @param <O> The type of the output data set items
 */
public interface FlatMapOp<I, O> extends ListOp<I, O> {
  /**
   * Given some input, produce an {@link Iterator} of outputs
   *
   * @param input The value to derive multiple outputs for
   * @return An {@link Iterator} of outputs
   */
  Iterator<O> flatMap(I input);

  @Override
  default IteratorWithResources<O> apply(IteratorWithResources<I> input) throws Exception {
    return new FlatMappingIteratorWithResources<>(input, this::flatMap);
  }
}
