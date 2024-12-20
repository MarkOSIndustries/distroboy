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
   * @param <I> The type of data in the {@link DataSource}
   * @param <O> The type of data the sequence will produce when applied
   * @param <CO> The type of data the sequence will collect on the cluster leader
   */
  public static class Builder<I, O, CO> {
    /** The {@link DataSource} this operation sequence will draw from */
    protected final DataSource<I> dataSource;
    /** The current end of the operation sequence */
    protected final Operand<O, CO> operand;

    /**
     * Start a new {@link DistributedOpSequence} builder
     *
     * @param dataSource The {@link DataSource} for the operation sequence to start from
     * @param operand The last operand in the chain of operations
     */
    Builder(DataSource<I> dataSource, Operand<O, CO> operand) {
      this.dataSource = dataSource;
      this.operand = operand;
    }

    /**
     * Generic extension point for using arbitrary operations external to DistroBoy.
     *
     * @param op The operation to apply next
     * @param <O2> The new output type of the sequence
     * @param <C2> The new collected output type of the sequence
     * @return A new {@link DistributedOpSequence.Builder} with this operation applied at the end
     */
    public <O2, C2> Builder<I, O2, C2> then(Operation<O, O2, C2> op) {
      return new Builder<>(dataSource, operand.then(op));
    }

    /**
     * Transforms each item via the given {@link MapOp}
     *
     * @param mapOp the mapping operation
     * @param <O2> The new output type of the sequence
     * @return A new {@link DistributedOpSequence.Builder} with this operation applied at the end
     */
    public <O2> Builder<I, O2, List<O2>> map(MapOp<O, O2> mapOp) {
      return new Builder<>(dataSource, operand.then(mapOp));
    }

    /**
     * Transforms each item via the given {@link MapOpWithResources}, closing the attached resources
     * when done.
     *
     * @param mapOp the mapping operation
     * @param <O2> The new output type of the sequence
     * @return A new {@link DistributedOpSequence.Builder} with this operation applied at the end
     */
    public <O2> Builder<I, O2, List<O2>> mapWithResources(MapOpWithResources<O, O2> mapOp) {
      return new Builder<>(dataSource, operand.then(mapOp));
    }

    /**
     * Transforms each item via the given {@link MapOp}, where the output type is an {@link
     * IteratorWithResources}
     *
     * @param mapOp the mapping operation
     * @param <O2> The new output type of the sequence
     * @param <O2Iterator> The Iterator type of the new output type
     * @return A new {@link DistributedOpSequence.IteratorBuilder} with this operation applied at
     *     the end
     */
    public <O2, O2Iterator extends Iterator<O2>>
        IteratorBuilder<I, O2, O2Iterator, List<O2Iterator>> mapToIterators(
            MapOp<O, O2Iterator> mapOp) {
      return new IteratorBuilder<>(dataSource, operand.then(mapOp));
    }

    /**
     * Transforms each item via the given {@link MapOp}, where the output type is an {@link
     * Iterable}
     *
     * @param mapOp the mapping operation
     * @param <O2> The new output type of the sequence
     * @param <O2Iterable> The Iterable type of the new output type
     * @return A new {@link DistributedOpSequence.IteratorBuilder} with this operation applied at
     *     the end
     */
    public <O2, O2Iterable extends Iterable<O2>>
        IteratorBuilder<I, O2, Iterator<O2>, List<Iterator<O2>>> mapToIterables(
            MapOp<O, O2Iterable> mapOp) {
      return new IteratorBuilder<>(
          dataSource,
          operand.then(mapOp).then((MapOp<O2Iterable, Iterator<O2>>) IteratorWithResources::from));
    }

    /**
     * Transforms each item via the given {@link FlatMapOp}, then flattens the results into a single
     * Iterator
     *
     * @param flatMapOp the flat-mapping operation
     * @param <O2> The new output type of the sequence
     * @return A new {@link DistributedOpSequence.Builder} with this operation applied at the end
     */
    public <O2> Builder<I, O2, List<O2>> flatMap(FlatMapOp<O, O2> flatMapOp) {
      return new Builder<>(dataSource, operand.then(flatMapOp));
    }

    /**
     * Transforms each item via the given {@link FlatMapOp}, then flattens the results into a single
     * Iterator, where the output type is an {@link Iterator}
     *
     * @param flatMapOp the flat-mapping operation
     * @param <O2> The new output type of the sequence
     * @param <O2Iterator> The Iterator type of the new output type
     * @return A new {@link DistributedOpSequence.Builder} with this operation applied at the end
     */
    public <O2, O2Iterator extends Iterator<O2>>
        IteratorBuilder<I, O2, O2Iterator, List<O2Iterator>> flatMapToIterators(
            FlatMapOp<O, O2Iterator> flatMapOp) {
      return new IteratorBuilder<>(dataSource, operand.then(flatMapOp));
    }

    /**
     * Transforms each item via the given {@link FlatMapOp}, then flattens the results into a single
     * Iterator, where the output type is an {@link Iterable}
     *
     * @param flatMapOp the flat-mapping operation
     * @param <O2> The new output type of the sequence
     * @param <O2Iterable> The Iterable type of the new output type
     * @return A new {@link DistributedOpSequence.Builder} with this operation applied at the end
     */
    public <O2, O2Iterable extends Iterable<O2>>
        IteratorBuilder<I, O2, Iterator<O2>, List<Iterator<O2>>> flatMapToIterables(
            FlatMapOp<O, O2Iterable> flatMapOp) {
      return new IteratorBuilder<>(
          dataSource,
          operand
              .then(flatMapOp)
              .then((MapOp<O2Iterable, Iterator<O2>>) IteratorWithResources::from));
    }

    /**
     * Aggregates each item via the given {@link ReduceOp}
     *
     * @param reduceOp the reducing operation
     * @param <O2> The new output type of the sequence
     * @return A new {@link DistributedOpSequence.Builder} with this operation applied at the end
     */
    public <O2> Builder<I, O2, O2> reduce(ReduceOp<O, O2> reduceOp) {
      return new Builder<>(dataSource, operand.then(reduceOp));
    }

    /**
     * Aggregates each item via the given {@link ReduceOp}, where the output type is an {@link
     * Iterator}
     *
     * @param reduceOp the reducing operation
     * @param <O2> The new output type of the sequence
     * @param <O2Iterator> The Iterator type of the new output type
     * @return A new {@link DistributedOpSequence.Builder} with this operation applied at the end
     */
    public <O2, O2Iterator extends Iterator<O2>>
        IteratorBuilder<I, O2, O2Iterator, O2Iterator> reduceToIterators(
            ReduceOp<O, O2Iterator> reduceOp) {
      return new IteratorBuilder<>(dataSource, operand.then(reduceOp));
    }

    /**
     * Aggregates each item via the given {@link ReduceOp}, where the output type is an {@link
     * Iterable}
     *
     * @param reduceOp the reducing operation
     * @param <O2> The new output type of the sequence
     * @param <O2Iterable> The Iterable type of the new output type
     * @return A new {@link DistributedOpSequence.Builder} with this operation applied at the end
     */
    public <O2, O2Iterable extends Iterable<O2>>
        IteratorBuilder<I, O2, Iterator<O2>, List<Iterator<O2>>> reduceToIterables(
            ReduceOp<O, O2Iterable> reduceOp) {
      return new IteratorBuilder<>(
          dataSource,
          operand
              .then(reduceOp)
              .then((MapOp<O2Iterable, Iterator<O2>>) IteratorWithResources::from));
    }

    /**
     * Filter each item via the given {@link FilterOp}
     *
     * @param filterOp the filtering operation
     * @return A new {@link DistributedOpSequence.Builder} with this operation applied at the end
     */
    public Builder<I, O, List<O>> filter(FilterOp<O> filterOp) {
      return new Builder<>(dataSource, operand.then(filterOp));
    }

    /**
     * Partition the items in the data set into batches of the given batch size
     *
     * @param batchSize the max size for a batch
     * @return A new {@link DistributedOpSequence.Builder} with this operation applied at the end
     */
    public Builder<I, List<O>, List<List<O>>> batch(int batchSize) {
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
    public DistributedOpSequence<I, O, CO> collect(Serialiser<O> serialiser) {
      return new DistributedOpSequence<>(dataSource, operand, serialiser);
    }

    /**
     * Shorthand for {@code .reduce(new Count<>()).collect(Serialiser.longValues)}. Count all
     * elements in the data set.
     *
     * @return A new {@link DistributedOpSequence} which will count the output set of the op
     *     sequence.
     */
    public DistributedOpSequence<I, Long, Long> count() {
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
    public DistributedOpSequence<I, DataReference, PersistedDataReferenceList<O>> persistToHeap(
        Cluster cluster, Serialiser<O> serialiser) {
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
    public DistributedOpSequence<I, DataReference, PersistedDataReferenceList<O>> persistToDisk(
        Cluster cluster, Serialiser<O> serialiser) {
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
    public DistributedOpSequence<I, DataReference, SortedDataReferenceList<O>> persistAndSortToDisk(
        Cluster cluster, Serialiser<O> serialiser, Comparator<O> comparator) {
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
    public <K> HashMapBuilder<Integer, K, List<O>> redistributeAndGroupBy(
        Cluster cluster,
        Function<O, K> classifier,
        Function<K, Integer> hasher,
        int partitions,
        Serialiser<O> serialiser)
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
    public DistributedOpSequence<I, Void, Void> forEach(ForEachOp<O> forEachOp) {
      return new DistributedOpSequence<>(
          dataSource, operand.then(forEachOp), Serialisers.voidValues);
    }
  }

  /**
   * A {@link DistributedOpSequence.Builder} with some extra convenience methods available for
   * dealing with {@link Iterator} items.
   *
   * @param <I> The type of data in the data source
   * @param <O> The type of data the resulting iterators contain
   * @param <OI> The type of the resulting iterators
   * @param <C> The type of data which would be collected by the current op sequence
   */
  public static class IteratorBuilder<I, O, OI extends Iterator<O>, C> extends Builder<I, OI, C> {
    IteratorBuilder(DataSource<I> dataSource, Operand<OI, C> operand) {
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
    public <K> HashMapBuilder<I, K, List<O>> groupBy(GroupByOp<O, OI, K> groupByOp) {
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
    public IteratorBuilder<I, O, OI, List<OI>> materialise() {
      return new IteratorBuilder<>(dataSource, operand.then(new MaterialiseOp<>()));
    }

    /**
     * Shortcut for {@code flatMap(x -> x)}
     *
     * @return A new {@link DistributedOpSequence.Builder} with this operation applied at the end
     */
    public Builder<I, O, List<O>> flatten() {
      return flatMap(x -> x);
    }
  }

  /**
   * A {@link DistributedOpSequence.Builder} with some extra convenience methods available for
   * dealing with {@link Map.Entry} items.
   *
   * @param <I> The type of data in the data source
   * @param <K> The type of keys in the {@link Map.Entry}s
   * @param <V> The type of values in the {@link Map.Entry}s
   */
  public static class HashMapBuilder<I, K, V> extends Builder<I, Map.Entry<K, V>, Map<K, V>> {
    HashMapBuilder(DataSource<I> dataSource, Operand<Map.Entry<K, V>, Map<K, V>> operand) {
      super(dataSource, operand);
    }

    /**
     * Transform the keys for each item in the data set (leaving the values alone)
     *
     * @param hashMapKeysOp The key mapping operation
     * @param <K2> The new type of the keys
     * @return A new {@link HashMapBuilder} with this operation applied at the end
     */
    public <K2> HashMapBuilder<I, K2, V> mapKeys(HashMapKeysOp<K, V, K2> hashMapKeysOp) {
      return new HashMapBuilder<>(dataSource, operand.then(hashMapKeysOp));
    }

    /**
     * Transform the value for each item in the data set (leaving the keys alone)
     *
     * @param hashMapValuesOp The key mapping operation
     * @param <V2> The new type of the values
     * @return A new {@link HashMapBuilder} with this operation applied at the end
     */
    public <V2> HashMapBuilder<I, K, V2> mapValues(HashMapValuesOp<K, V, V2> hashMapValuesOp) {
      return new HashMapBuilder<>(dataSource, operand.then(hashMapValuesOp));
    }

    /**
     * Transform the keys and values for each item in the data set independently of each other If
     * you need to map the keys and values in context of each other - use {@link
     * #map(HashMapToListOp)}
     *
     * @param hashMapKeysAndValuesOp The key/value mapping operation
     * @param <K2> The new type of the keys
     * @param <V2> The new type of the values
     * @return A new {@link HashMapBuilder} with this operation applied at the end
     */
    public <K2, V2> HashMapBuilder<I, K2, V2> mapKeysAndValues(
        HashMapKeysAndValuesOp<K, V, K2, V2> hashMapKeysAndValuesOp) {
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
    public <O> Builder<I, O, List<O>> map(HashMapToListOp<K, V, O> mapOp) {
      return new Builder<>(dataSource, operand.then(mapOp));
    }
  }
}
