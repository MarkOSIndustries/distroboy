package com.markosindustries.distroboy.core.operations;

import com.google.common.collect.Iterators;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import java.util.Iterator;
import java.util.List;

/**
 * An operation which groups inputs into batches of a given size.
 *
 * @see Iterators#partition(Iterator, int)
 * @param <I> The type of data provided as input
 */
public class BatchOp<I> implements ListOp<I, List<I>> {
  private final int batchSize;

  /**
   * An operation which groups inputs into batches of a given size
   *
   * @param batchSize The maximum size each batch should be
   */
  public BatchOp(int batchSize) {
    this.batchSize = batchSize;
  }

  @Override
  public IteratorWithResources<List<I>> apply(IteratorWithResources<I> input) throws Exception {
    return IteratorWithResources.from(Iterators.partition(input, batchSize), input);
  }
}
