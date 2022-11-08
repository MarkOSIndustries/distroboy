package com.markosindustries.distroboy.core.operations;

import java.util.Collection;
import java.util.function.Supplier;

/**
 * Reduce a distributed data set by putting all the items on each node into a collection using the
 * provided collection constructor. If collected after this stage, the results on the leader node
 * will also use the specified collection type and constructor.
 *
 * @param <I> The input data type
 * @param <C> The collection type
 */
public class ReduceToCollection<I, C extends Collection<I>> implements ReduceOp<I, C> {
  private final Supplier<C> newCollection;

  /**
   * @see ReduceToCollection
   * @param newCollection The constructor to create an empty collection
   */
  public ReduceToCollection(Supplier<C> newCollection) {
    this.newCollection = newCollection;
  }

  @Override
  public C initAggregate() {
    return newCollection.get();
  }

  @Override
  public C reduceInput(final C aggregate, final I input) throws Exception {
    aggregate.add(input);
    return aggregate;
  }

  @Override
  public C reduceOutput(final C aggregate, final C result) {
    aggregate.addAll(result);
    return aggregate;
  }
}
