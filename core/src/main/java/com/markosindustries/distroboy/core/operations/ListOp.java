package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.IteratorTo;
import java.util.Iterator;
import java.util.List;

/**
 * An operation which collects a {@link List} of its outputs
 *
 * @param <I> The type of data provided as input
 * @param <O> The type of data being produced by this operation
 */
public interface ListOp<I, O> extends Operation<I, O, List<O>> {
  @Override
  default List<O> collect(Iterator<O> results) {
    return IteratorTo.list(results);
  }
}
