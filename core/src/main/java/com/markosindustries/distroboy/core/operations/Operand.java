package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.schemas.DataSourceRange;
import java.util.Iterator;
import java.util.List;

/**
 * Represents a data set which can be used with a {@link DistributedOpSequence} on a distroboy
 * cluster
 *
 * @param <I> The type of items in the data set
 * @param <CI> The type of items which will be collected if the dataset is not transformed
 */
public interface Operand<I, CI> {
  default <O, CO> Operand<O, CO> then(Operation<I, O, CO> operation) {
    return new AppliedOperation<>(this, operation);
  }

  List<Operand<?, ?>> dependencies();

  IteratorWithResources<I> enumerateRangeForNode(DataSourceRange dataSourceRange) throws Exception;

  CI collect(Iterator<I> results);
}
