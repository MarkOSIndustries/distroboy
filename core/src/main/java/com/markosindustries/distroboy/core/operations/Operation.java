package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import java.util.Iterator;

/**
 * The basic interface which all distributed operations must satisfy.
 *
 * @param <I> The input data set type
 * @param <O> The output data set type
 * @param <C> The type of the data that would be collected after this operation
 */
public interface Operation<I, O, C> {
  /**
   * Given an input {@link IteratorWithResources}, produce a new {@link IteratorWithResources} which
   * is the result of applying this operation to the inputs.
   *
   * @param input An iterator over the inputs
   * @return An iterator over the outputs of this operation
   * @throws Exception if the operation fails
   */
  IteratorWithResources<O> apply(IteratorWithResources<I> input) throws Exception;

  /**
   * Given an iterator of outputs, transform it to the collection type. A common example is the
   * {@link ListOp} implementation which simply materialises the {@link Iterator} as a {@link
   * java.util.List}
   *
   * @param results An iterator over the results of the operation.
   * @return The collected results
   */
  C collect(Iterator<O> results);
}
