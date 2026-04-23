package com.markosindustries.distroboy.core.operations;

import com.markosindustries.distroboy.core.Cluster;
import com.markosindustries.distroboy.core.Count;
import com.markosindustries.distroboy.core.PersistedDataReferenceList;
import com.markosindustries.distroboy.core.SortedDataReferenceList;
import com.markosindustries.distroboy.core.clustering.serialisation.ProtobufValues;
import com.markosindustries.distroboy.core.clustering.serialisation.Serialiser;
import com.markosindustries.distroboy.core.clustering.serialisation.Serialisers;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.schemas.DataReference;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A sequence of operations to perform on a distroboy cluster. This is the main interface for
 * defining operations that a distributed job will perform.
 *
 * @param <Input> The type of data being provided to the operation sequence (as a {@link DataSource}
 * @param <Outcome> The type of data which the sequence will produce when applied.
 * @param <CollectedOutcome> The type of data which will be collected on the cluster leader once the
 *     sequence is complete.
 */
public class DistributedOpSequence<Input, Outcome, CollectedOutcome> {
  private final DataSource<Input> dataSource;
  private final Operand<Outcome, CollectedOutcome> operand;
  private final Serialiser<Outcome> serialiser;

  private DistributedOpSequence(
      DataSource<Input> dataSource,
      Operand<Outcome, CollectedOutcome> operand,
      Serialiser<Outcome> serialiser) {
    this.dataSource = dataSource;
    this.operand = operand;
    this.serialiser = serialiser;
  }

  /**
   * The {@link DataSource} this operation sequence will draw from
   *
   * @return The {@link DataSource}
   */
  public DataSource<Input> getDataSource() {
    return dataSource;
  }

  /**
   * The tail of the operation sequence
   *
   * @return The last operand in the sequence
   */
  public Operand<Outcome, CollectedOutcome> getOperand() {
    return operand;
  }

  /**
   * The serialiser required to move the Outcome values of this sequence around the cluster
   *
   * @return The serialiser
   */
  public Serialiser<Outcome> getSerialiser() {
    return serialiser;
  }

  /**
   * Start a new {@link DistributedOpSequence.Builder} which will read from the given {@link
   * DataSource}
   *
   * @param dataSource The data source the operation sequence will process
   * @param <I> The type of data in the {@link DataSource}
   * @param <DS> The type of the {@link DataSource}
   * @return A new {@link DistributedOpSequence.Builder}
   */
  public static <I, DS extends Operand<I, List<I>> & DataSource<I>> Builder<I, I, List<I>> readFrom(
      DS dataSource) {
    return new Builder<>(dataSource, dataSource);
  }

  /**
   * Builder for a {@link DistributedOpSequence}
   *
   * @param <Input> The type of data in the {@link DataSource}
   * @param <Output> The type of data the sequence will produce when applied
   * @param <CollectedOutput> The type of data the sequence will collect on the cluster leader
   */
  public static class Builder<Input, Output, CollectedOutput> {
    /** The {@link DataSource} this operation sequence will draw from */
    protected final DataSource<Input> dataSource;
    /** The current end of the operation sequence */
    protected final Operand<Output, CollectedOutput> operand;

    /**
     * Start a new {@link DistributedOpSequence} builder
     *
     * @param dataSource The {@link DataSource} for the operation sequence to start from
     * @param operand The last operand in the chain of operations
     */
    Builder(DataSource<Input> dataSource, Operand<Output, CollectedOutput> operand) {
      this.dataSource = dataSource;
      this.operand = operand;
    }

    /**
     * Generic extension point for using arbitrary operations external to DistroBoy.
     *
     * @param op The operation to apply next
     * @param <NewOuput> The new output type of the sequence
     * @param <NewCollectedOuput> The new collected output type of the sequence
     * @return A new {@link DistributedOpSequence.Builder} with this operation applied at the end
     */
    public <NewOuput, NewCollectedOuput> Builder<Input, NewOuput, NewCollectedOuput> then(
        Operation<Output, NewOuput, NewCollectedOuput> op) {
      return new Builder<>(dataSource, operand.then(op));
    }

    /**
     * Transforms each item via the given {@link MapOp}
     *
     * @param mapOp the mapping operation
     * @param <NewOuput> The new output type of the sequence
     * @return A new {@link DistributedOpSequence.Builder} with this operation applied at the end
     */
    public <NewOuput> Builder<Input, NewOuput, List<NewOuput>> map(MapOp<Output, NewOuput> mapOp) {
      return new Builder<>(dataSource, operand.then(mapOp));
    }

    /**
     * Transforms each item via the given {@link MapOpWithResources}, closing the attached resources
     * when done.
     *
     * @param mapOp the mapping operation
     * @param <NewOuput> The new output type of the sequence
     * @return A new {@link DistributedOpSequence.Builder} with this operation applied at the end
     */
    public <NewOuput> Builder<Input, NewOuput, List<NewOuput>> mapWithResources(
        MapOpWithResources<Output, NewOuput> mapOp) {
      return new Builder<>(dataSource, operand.then(mapOp));
    }

    /**
     * Transforms each item via the given {@link MapOp}, where the output type is an {@link
     * IteratorWithResources}
     *
     * @param mapOp the mapping operation
     * @param <NewOuput> The new output type of the sequence
     * @param <NewOuputIterator> The Iterator type of the new output type
     * @return A new {@link DistributedOpSequence.IteratorBuilder} with this operation applied at
     *     the end
     */
    public <NewOuput, NewOuputIterator extends Iterator<NewOuput>>
        IteratorBuilder<Input, NewOuput, NewOuputIterator, List<NewOuputIterator>> mapToIterators(
            MapOp<Output, NewOuputIterator> mapOp) {
      return new IteratorBuilder<>(dataSource, operand.then(mapOp));
    }

    /**
     * Transforms each item via the given {@link MapOp}, where the output type is an {@link
     * Iterable}
     *
     * @param mapOp the mapping operation
     * @param <NewOuput> The new output type of the sequence
     * @param <NewOuputIterable> The Iterable type of the new output type
     * @return A new {@link DistributedOpSequence.IteratorBuilder} with this operation applied at
     *     the end
     */
    public <NewOuput, NewOuputIterable extends Iterable<NewOuput>>
        IteratorBuilder<Input, NewOuput, Iterator<NewOuput>, List<Iterator<NewOuput>>>
            mapToIterables(MapOp<Output, NewOuputIterable> mapOp) {
      return new IteratorBuilder<>(
          dataSource,
          operand
              .then(mapOp)
              .then((MapOp<NewOuputIterable, Iterator<NewOuput>>) IteratorWithResources::from));
    }

    /**
     * Transforms each item via the given {@link FlatMapOp}, then flattens the results into a single
     * Iterator
     *
     * @param flatMapOp the flat-mapping operation
     * @param <NewOuput> The new output type of the sequence
     * @return A new {@link DistributedOpSequence.Builder} with this operation applied at the end
     */
    public <NewOuput> Builder<Input, NewOuput, List<NewOuput>> flatMap(
        FlatMapOp<Output, NewOuput> flatMapOp) {
      return new Builder<>(dataSource, operand.then(flatMapOp));
    }

    /**
     * Transforms each item via the given {@link FlatMapOp}, then flattens the results into a single
     * Iterator, where the output type is an {@link Iterator}
     *
     * @param flatMapOp the flat-mapping operation
     * @param <NewOuput> The new output type of the sequence
     * @param <NewOuputIterator> The Iterator type of the new output type
     * @return A new {@link DistributedOpSequence.Builder} with this operation applied at the end
     */
    public <NewOuput, NewOuputIterator extends Iterator<NewOuput>>
        IteratorBuilder<Input, NewOuput, NewOuputIterator, List<NewOuputIterator>>
            flatMapToIterators(FlatMapOp<Output, NewOuputIterator> flatMapOp) {
      return new IteratorBuilder<>(dataSource, operand.then(flatMapOp));
    }

    /**
     * Transforms each item via the given {@link FlatMapOp}, then flattens the results into a single
     * Iterator, where the output type is an {@link Iterable}
     *
     * @param flatMapOp the flat-mapping operation
     * @param <NewOuput> The new output type of the sequence
     * @param <NewOuputIterable> The Iterable type of the new output type
     * @return A new {@link DistributedOpSequence.Builder} with this operation applied at the end
     */
    public <NewOuput, NewOuputIterable extends Iterable<NewOuput>>
        IteratorBuilder<Input, NewOuput, Iterator<NewOuput>, List<Iterator<NewOuput>>>
            flatMapToIterables(FlatMapOp<Output, NewOuputIterable> flatMapOp) {
      return new IteratorBuilder<>(
          dataSource,
          operand
              .then(flatMapOp)
              .then((MapOp<NewOuputIterable, Iterator<NewOuput>>) IteratorWithResources::from));
    }

    /**
     * Aggregates each item via the given {@link ReduceOp}
     *
     * @param reduceOp the reducing operation
     * @param <NewOuput> The new output type of the sequence
     * @return A new {@link DistributedOpSequence.Builder} with this operation applied at the end
     */
    public <NewOuput> Builder<Input, NewOuput, NewOuput> reduce(
        ReduceOp<Output, NewOuput> reduceOp) {
      return new Builder<>(dataSource, operand.then(reduceOp));
    }

    /**
     * Aggregates each item via the given {@link ReduceOp}, where the output type is an {@link
     * Iterator}
     *
     * @param reduceOp the reducing operation
     * @param <NewOuput> The new output type of the sequence
     * @param <NewOuputIterator> The Iterator type of the new output type
     * @return A new {@link DistributedOpSequence.Builder} with this operation applied at the end
     */
    public <NewOuput, NewOuputIterator extends Iterator<NewOuput>>
        IteratorBuilder<Input, NewOuput, NewOuputIterator, NewOuputIterator> reduceToIterators(
            ReduceOp<Output, NewOuputIterator> reduceOp) {
      return new IteratorBuilder<>(dataSource, operand.then(reduceOp));
    }

    /**
     * Aggregates each item via the given {@link ReduceOp}, where the output type is an {@link
     * Iterable}
     *
     * @param reduceOp the reducing operation
     * @param <NewOuput> The new output type of the sequence
     * @param <NewOuputIterable> The Iterable type of the new output type
     * @return A new {@link DistributedOpSequence.Builder} with this operation applied at the end
     */
    public <NewOuput, NewOuputIterable extends Iterable<NewOuput>>
        IteratorBuilder<Input, NewOuput, Iterator<NewOuput>, List<Iterator<NewOuput>>>
            reduceToIterables(ReduceOp<Output, NewOuputIterable> reduceOp) {
      return new IteratorBuilder<>(
          dataSource,
          operand
              .then(reduceOp)
              .then((MapOp<NewOuputIterable, Iterator<NewOuput>>) IteratorWithResources::from));
    }

    /**
     * Filter each item via the given {@link FilterOp}
     *
     * @param filterOp the filtering operation
     * @return A new {@link DistributedOpSequence.Builder} with this operation applied at the end
     */
    public Builder<Input, Output, List<Output>> filter(FilterOp<Output> filterOp) {
      return new Builder<>(dataSource, operand.then(filterOp));
    }

    /**
     * Partition the items in the data set into batches of the given batch size
     *
     * @param batchSize the max size for a batch
     * @return A new {@link DistributedOpSequence.Builder} with this operation applied at the end
     */
    public Builder<Input, List<Output>, List<List<Output>>> batch(int batchSize) {
      return new Builder<>(dataSource, operand.then(new BatchOp<>(batchSize)));
    }

    /**
     * Finish building the {@link DistributedOpSequence} and return it, ready to execute on a
     * distroboy cluster.
     *
     * @param serialiser A serialiser for the output type of the sequence.
     * @return A new {@link DistributedOpSequence} which will use the given serialiser to return the
     *     collected data to the cluster leader.
     */
    public DistributedOpSequence<Input, Output, CollectedOutput> collect(
        Serialiser<Output> serialiser) {
      return new DistributedOpSequence<>(dataSource, operand, serialiser);
    }

    /**
     * Shorthand for {@code .reduce(new Count<>()).collect(Serialiser.longValues)}. Count all
     * elements in the data set.
     *
     * @return A new {@link DistributedOpSequence} which will count the output set of the op
     *     sequence.
     */
    public DistributedOpSequence<Input, Long, Long> count() {
      return new DistributedOpSequence<>(
          dataSource, operand.then(new Count<>()), Serialisers.longValues);
    }

    /**
     * Have each node in the cluster persist its fragment of the data to the heap. <b>WARNING:</b>
     * if the data doesn't fit in the available heap memory, the entire job will fail.
     *
     * @param cluster The {@link Cluster} on which data is being persisted
     * @param serialiser A {@link Serialiser} for the data being persisted
     * @return A new {@link DistributedOpSequence} whose result will be a set of {@link
     *     DataReference}s to the data stored on each node
     */
    public DistributedOpSequence<Input, DataReference, PersistedDataReferenceList<Output>>
        persistToHeap(Cluster cluster, Serialiser<Output> serialiser) {
      return new DistributedOpSequence<>(
          dataSource,
          operand.then(new PersistToHeap<>(cluster, serialiser)),
          new ProtobufValues<>(DataReference::parseFrom));
    }

    /**
     * Have each node in the cluster persist its fragment of the data to disk. <b>WARNING:</b> if
     * the data doesn't fit in the available temp dir space, the entire job will fail.
     *
     * @param cluster The {@link Cluster} on which data is being persisted
     * @param serialiser A {@link Serialiser} for the data being persisted
     * @return A new {@link DistributedOpSequence} whose result will be a set of {@link
     *     DataReference}s to the data stored on each node
     */
    public DistributedOpSequence<Input, DataReference, PersistedDataReferenceList<Output>>
        persistToDisk(Cluster cluster, Serialiser<Output> serialiser) {
      return new DistributedOpSequence<>(
          dataSource,
          operand.then(new PersistToDisk<>(cluster, serialiser)),
          new ProtobufValues<>(DataReference::parseFrom));
    }

    /**
     * Have each node in the cluster persist and locally sort its fragment of the data to disk.
     * <b>WARNING:</b> if the data doesn't fit in the available temp dir space, the entire job will
     * fail.
     *
     * @param cluster The {@link Cluster} on which data is being persisted
     * @param serialiser A {@link Serialiser} for the data being persisted
     * @return A new {@link DistributedOpSequence} whose result will be a set of {@link
     *     DataReference}s to the data stored on each node
     */
    public DistributedOpSequence<Input, DataReference, SortedDataReferenceList<Output>>
        persistAndSortToDisk(
            Cluster cluster, Serialiser<Output> serialiser, Comparator<Output> comparator) {
      return new DistributedOpSequence<>(
          dataSource,
          operand.then(new PersistSortedToDisk<>(cluster, serialiser, comparator)),
          new ProtobufValues<>(DataReference::parseFrom));
    }

    /**
     * Perform an efficient once-only redistribution and groupBy via distributed iterator
     * references.
     *
     * @param cluster The cluster which should be used to redistribute based on the keys classified
     * @param classifier Given an input I, returns a grouping key K, which will be hashed
     * @param hasher A function which takes the grouping key K, and hashes it to a number in Integer
     *     space
     * @param partitions The number of partitions desired (ie: the modulus to use for the hashes)
     * @param serialiser A {@link Serialiser} for the data being moved around the cluster.
     * @param <K> The type of the key to group by (and which will be hashed for redistribution)
     * @return A new {@link DistributedOpSequence.HashMapBuilder}
     * @throws InterruptedException if interrupted while orchestrating the redistribution
     */
    public <K> HashMapBuilder<Integer, K, List<Output>> redistributeAndGroupBy(
        Cluster cluster,
        Function<Output, K> classifier,
        Function<K, Integer> hasher,
        int partitions,
        Serialiser<Output> serialiser)
        throws InterruptedException {
      final var dataReferences =
          cluster.distributeReferences(
              new DistributedOpSequence<>(
                  dataSource,
                  operand.then(new GetIteratorReferences<>(cluster, serialiser)),
                  new ProtobufValues<>(DataReference::parseFrom)));

      return cluster.redistributeAndGroupBy(
          dataReferences, classifier, hasher, partitions, serialiser);
    }

    /**
     * Have each node loop over its fragment of the dataset performing the given operation. Useful
     * when you don't need to aggregate or collect any results, but want to have all the nodes in
     * the cluster running the given code.
     *
     * @param forEachOp The body of the distributed for-each loop
     * @return A {@link DistributedOpSequence} which when run will apply the given for-each loop at
     *     the end.
     */
    public DistributedOpSequence<Input, Void, Void> forEach(ForEachOp<Output> forEachOp) {
      return new DistributedOpSequence<>(
          dataSource, operand.then(forEachOp), Serialisers.voidValues);
    }
  }

  /**
   * A {@link DistributedOpSequence.Builder} with some extra convenience methods available for
   * dealing with {@link Iterator} items.
   *
   * @param <Input> The type of data in the data source
   * @param <Output> The type of data the resulting iterators contain
   * @param <OutputIterator> The type of the resulting iterators
   * @param <CollectedOutput> The type of data which would be collected by the current op sequence
   */
  public static class IteratorBuilder<
          Input, Output, OutputIterator extends Iterator<Output>, CollectedOutput>
      extends Builder<Input, OutputIterator, CollectedOutput> {
    IteratorBuilder(
        DataSource<Input> dataSource, Operand<OutputIterator, CollectedOutput> operand) {
      super(dataSource, operand);
    }

    /**
     * Have each node in the cluster group its fragment of the data set by a given key classifier.
     * If this operation hasn't been preceded by redistributing the data across the cluster using
     * the same key classifier to generate hashes, this operation will likely not make any sense.
     *
     * @param groupByOp The group by operation spec
     * @param <K> The type of the keys which will be grouped on
     * @return A new {@link HashMapBuilder} with this operation applied at the end
     */
    public <K> HashMapBuilder<Input, K, List<Output>> groupBy(
        GroupByOp<Output, OutputIterator, K> groupByOp) {
      return new HashMapBuilder<>(dataSource, operand.then(groupByOp));
    }

    /**
     * Force each node to generate a {@link List} from the {@link java.util.Iterator} normally used
     * while processing data sets. While this is useful to ensure that preceding steps are complete
     * for all data items, it does mean the entire data set at this point must fit into working
     * memory.
     *
     * @return A new {@link IteratorBuilder} with this operation applied at the end
     */
    public IteratorBuilder<Input, Output, OutputIterator, List<OutputIterator>> materialise() {
      return new IteratorBuilder<>(dataSource, operand.then(new MaterialiseOp<>()));
    }

    /**
     * Shortcut for {@code flatMap(x -> x)}
     *
     * @return A new {@link DistributedOpSequence.Builder} with this operation applied at the end
     */
    public Builder<Input, Output, List<Output>> flatten() {
      return flatMap(x -> x);
    }
  }

  /**
   * A {@link DistributedOpSequence.Builder} with some extra convenience methods available for
   * dealing with {@link Map.Entry} items.
   *
   * @param <Input> The type of data in the data source
   * @param <Key> The type of keys in the {@link Map.Entry}s
   * @param <Value> The type of values in the {@link Map.Entry}s
   */
  public static class HashMapBuilder<Input, Key, Value>
      extends Builder<Input, Map.Entry<Key, Value>, Map<Key, Value>> {
    HashMapBuilder(
        DataSource<Input> dataSource, Operand<Map.Entry<Key, Value>, Map<Key, Value>> operand) {
      super(dataSource, operand);
    }

    /**
     * Transform the keys for each item in the data set (leaving the values alone)
     *
     * @param hashMapKeysOp The key mapping operation
     * @param <NewKey> The new type of the keys
     * @return A new {@link HashMapBuilder} with this operation applied at the end
     */
    public <NewKey> HashMapBuilder<Input, NewKey, Value> mapKeys(
        HashMapKeysOp<Key, Value, NewKey> hashMapKeysOp) {
      return new HashMapBuilder<>(dataSource, operand.then(hashMapKeysOp));
    }

    /**
     * Transform the value for each item in the data set (leaving the keys alone)
     *
     * @param hashMapValuesOp The key mapping operation
     * @param <NewValue> The new type of the values
     * @return A new {@link HashMapBuilder} with this operation applied at the end
     */
    public <NewValue> HashMapBuilder<Input, Key, NewValue> mapValues(
        HashMapValuesOp<Key, Value, NewValue> hashMapValuesOp) {
      return new HashMapBuilder<>(dataSource, operand.then(hashMapValuesOp));
    }

    /**
     * Transform the keys and values for each item in the data set independently of each other If
     * you need to map the keys and values in context of each other - use {@link
     * #map(HashMapToListOp)}
     *
     * @param hashMapKeysAndValuesOp The key/value mapping operation
     * @param <NewKey> The new type of the keys
     * @param <NewValue> The new type of the values
     * @return A new {@link HashMapBuilder} with this operation applied at the end
     */
    public <NewKey, NewValue> HashMapBuilder<Input, NewKey, NewValue> mapKeysAndValues(
        HashMapKeysAndValuesOp<Key, Value, NewKey, NewValue> hashMapKeysAndValuesOp) {
      return new HashMapBuilder<>(dataSource, operand.then(hashMapKeysAndValuesOp));
    }

    /**
     * Transform each Map.Entry in the data set using the given mapping operation. Purely here for
     * the convenience of being able to provide a lambda which takes a key/value as separate
     * parameters.
     *
     * @param mapOp The entry mapping operation
     * @param <O> The output type of the mapping operation
     * @return A new {@link Builder} with this operation applied at the end
     */
    public <O> Builder<Input, O, List<O>> map(HashMapToListOp<Key, Value, O> mapOp) {
      return new Builder<>(dataSource, operand.then(mapOp));
    }
  }
}
