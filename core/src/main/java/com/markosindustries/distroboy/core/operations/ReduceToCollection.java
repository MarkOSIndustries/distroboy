package com.markosindustries.distroboy.core.operations;

import java.util.Collection;
import java.util.function.Supplier;

/**
 * Reduce a distributed data set by putting all the items on each node into a collection using the
 * provided collection constructor. If collected after this stage, the results on the leader node
 * will also use the specified collection type and constructor.
 *
 * @param <Input> The input data type
 * @param <CollectionType> The collection type
 */
public class ReduceToCollection<Input, CollectionType extends Collection<Input>>
    implements ReduceOp<Input, CollectionType> {
  private final Supplier<CollectionType> newCollection;

  /**
   * @see ReduceToCollection
   * @param newCollection The constructor to create an empty collection
   */
  public ReduceToCollection(Supplier<CollectionType> newCollection) {
    this.newCollection = newCollection;
  }

  @Override
  public CollectionType initAggregate() {
    return newCollection.get();
  }

  @Override
  public CollectionType reduceInput(final CollectionType aggregate, final Input input)
      throws Exception {
    aggregate.add(input);
    return aggregate;
  }

  @Override
  public CollectionType reduceOutput(final CollectionType aggregate, final CollectionType result) {
    aggregate.addAll(result);
    return aggregate;
  }
}
