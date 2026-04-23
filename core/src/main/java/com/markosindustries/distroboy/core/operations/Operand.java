package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.schemas.DataSourceRange;
import java.util.Iterator;
import java.util.List;

/**
 * Represents a data set which can be used with a {@link DistributedOpSequence} on a distroboy
 * cluster
 *
 * @param <Input> The type of items in the data set
 * @param <CollectedInput> The type of items which will be collected if the dataset is not
 *     transformed
 */
public interface Operand<Input, CollectedInput> {
  /**
   * Produce a new operand representing the result of applying the given operation to this operand
   *
   * @param operation The operation to apply
   * @param <Output> The output type of the operation
   * @param <CollectedOutput> The collected value type of the operation
   * @return A new {@link Operand}
   */
  default <Output, CollectedOutput> Operand<Output, CollectedOutput> then(
      Operation<Input, Output, CollectedOutput> operation) {
    return new AppliedOperation<>(this, operation);
  }

  /**
   * Get the list of {@link Operand}s upstream of this one
   *
   * @return A list of {@link Operand}s
   */
  List<Operand<?, ?>> dependencies();

  /**
   * Produce an iterator by requesting the given range of data from the upstream {@link DataSource}
   * and applying all preceding operations
   *
   * @param dataSourceRange The range of data requested from the {@link DataSource}
   * @return An iterator of values produced by processing the requested DataSource range through all
   *     preceding operations
   * @throws Exception If enumeration fails
   */
  IteratorWithResources<Input> enumerateRangeForNode(DataSourceRange dataSourceRange)
      throws Exception;

  /**
   * Given an iterator over the results of having produced this operand, transform them into the
   * collected value type. Will be called on the cluster leader node
   *
   * @param results The results of processing the data source as specified by this {@link Operand}
   *     and it's preceding {@link Operation}s
   * @return The collected data, ready to be handled on the cluster leader at the end of the job
   */
  CollectedInput collect(Iterator<Input> results);
}
