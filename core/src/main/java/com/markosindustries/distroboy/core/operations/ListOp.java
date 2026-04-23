package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.IteratorTo;
import java.util.Iterator;
import java.util.List;

/**
 * An operation which collects a {@link List} of its outputs
 *
 * @param <Input> The type of data provided as input
 * @param <Output> The type of data being produced by this operation
 */
public interface ListOp<Input, Output> extends Operation<Input, Output, List<Output>> {
  @Override
  default List<Output> collect(Iterator<Output> results) {
    return IteratorTo.list(results);
  }
}
