package com.markosindustries.distroboy.core.operations;

import static java.util.Objects.nonNull;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A high level interface for any operation which takes a distributed data set and reduces it down
 * to a single aggregate
 *
 * @param <Input> The type of the input data set items
 * @param <Output> The type of the output data set items
 */
public interface ReduceOp<Input, Output> extends Operation<Input, Output, Output> {
  /**
   * Prepare the aggregate to be used for reduction
   *
   * @return A fresh aggregate
   */
  default Output initAggregate() {
    return null;
  }

  /**
   * Convert a completed aggregate to an {@link IteratorWithResources}
   *
   * @param aggregate The completed aggregate
   * @return An iterator containing the given aggregate
   */
  default IteratorWithResources<Output> asIterator(Output aggregate) {
    return IteratorWithResources.from(
        nonNull(aggregate) ? List.of(aggregate).iterator() : Collections.emptyIterator());
  }

  /**
   * Apply a given input to the current aggregate
   *
   * @param aggregate The current state of the aggregate
   * @param input The next input to reduce
   * @return The resulting aggregate after applying the input
   * @throws Exception If producing a new aggregate fails
   */
  Output reduceInput(Output aggregate, Input input) throws Exception;

  /**
   * Combine two aggregates together to produce a single resulting aggregate.
   *
   * @param aggregate The current aggregate
   * @param result The foreign aggregate being merged in
   * @return The resulting aggregate
   */
  Output reduceOutput(Output aggregate, Output result);

  @Override
  default IteratorWithResources<Output> apply(IteratorWithResources<Input> input) throws Exception {
    try (input) {
      var aggregate = initAggregate();
      while (input.hasNext()) {
        aggregate = reduceInput(aggregate, input.next());
      }
      return asIterator(aggregate);
    }
  }

  @Override
  default Output collect(Iterator<Output> results) {
    var aggregate = initAggregate();
    while (results.hasNext()) {
      aggregate = reduceOutput(aggregate, results.next());
    }
    return aggregate;
  }
}
