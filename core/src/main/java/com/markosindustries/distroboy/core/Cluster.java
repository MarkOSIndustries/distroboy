package com.markosindustries.distroboy.core;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toUnmodifiableList;

import com.google.common.collect.Iterables;
import com.markosindustries.distroboy.core.clustering.ClusterMember;
import com.markosindustries.distroboy.core.clustering.ClusterMemberId;
import com.markosindustries.distroboy.core.clustering.DataReferenceId;
import com.markosindustries.distroboy.core.clustering.DistributableDataReference;
import com.markosindustries.distroboy.core.clustering.DistributedIterator;
import com.markosindustries.distroboy.core.clustering.serialisation.Serialiser;
import com.markosindustries.distroboy.core.clustering.serialisation.Serialisers;
import com.markosindustries.distroboy.core.iterators.FlatMappingIterator;
import com.markosindustries.distroboy.core.iterators.IteratorTo;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.iterators.MappingIteratorWithResources;
import com.markosindustries.distroboy.core.iterators.MergeSortingIteratorWithResources;
import com.markosindustries.distroboy.core.operations.DistributedOpSequence;
import com.markosindustries.distroboy.core.operations.EvenlyRedistributedDataSource;
import com.markosindustries.distroboy.core.operations.StaticDataSource;
import com.markosindustries.distroboy.schemas.DataReference;
import com.markosindustries.distroboy.schemas.DataReferenceHashSpec;
import com.markosindustries.distroboy.schemas.DataReferenceRange;
import com.markosindustries.distroboy.schemas.DataReferenceSortRange;
import com.markosindustries.distroboy.schemas.SortRange;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cluster represents the set of nodes which will be involved in processing data. Everything you can
 * do with distroboy starts with joining a cluster.
 */
public final class Cluster implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(Cluster.class);
  private final ClusterMember clusterMember;

  /**
   * Start a builder for a cluster
   *
   * @param clusterName The name all of the processing nodes will use to identify this job
   * @param expectedClusterMembers The number of processing nodes we expect to join us before
   *     starting
   * @return A new Cluster builder.
   */
  public static Builder newBuilder(String clusterName, int expectedClusterMembers) {
    return new Builder(clusterName, expectedClusterMembers);
  }

  /** Builder object for a Cluster. Used to configure the Cluster before joining. */
  public static class Builder {
    private final String clusterName;
    private final int expectedClusterMembers;
    private String coordinatorHost = "localhost";
    private int coordinatorPort = 7070;
    private Duration coordinatorLobbyTimeout = Duration.ofMinutes(5);
    private int memberPort = 7071;
    private int maxGrpcMessageSize =
        4 * 1024 * 1024; // this is the default in the NettyGrpcServer implementation

    private Builder(String clusterName, int expectedClusterMembers) {
      this.clusterName = clusterName;
      this.expectedClusterMembers = expectedClusterMembers;
    }

    /**
     * Set the address of the distroboy coordinator, which is required for discovering the rest of
     * the cluster members.
     *
     * @param host The host name/ip of the coordinator.
     * @param port The port the coordinator is listening on.
     * @return this
     */
    public Builder coordinator(String host, int port) {
      coordinatorHost = host;
      coordinatorPort = port;
      return this;
    }

    /**
     * Set the maximum time this node will wait for the rest of the cluster members to join
     *
     * @param timeout The timeout
     * @return this
     */
    public Builder lobbyTimeout(Duration timeout) {
      coordinatorLobbyTimeout = Duration.ofMillis(timeout.toMillis());
      return this;
    }

    /**
     * Set the port this cluster member will listen on
     *
     * @param port The port to listen on
     * @return this
     */
    public Builder memberPort(int port) {
      memberPort = port;
      return this;
    }

    /**
     * Set the maximum bytes allowed to be transmitted in one message across the cluster's mesh GRPC
     * connections
     *
     * @param maxGrpcMessageSize the maximum bytes allowed to be transmitted in one message across
     *     the cluster's mesh GRPC connections
     * @return this
     */
    public Builder maxGrpcMessageSize(int maxGrpcMessageSize) {
      this.maxGrpcMessageSize = maxGrpcMessageSize;
      return this;
    }

    /**
     * Join the cluster and await all other cluster members
     *
     * @return A Cluster ready to run distributed operations
     * @throws IOException If we fail to run the member RPC endpoint
     * @throws ExecutionException If the coordinator can't start the cluster
     * @throws InterruptedException If interrupted while joining the cluster
     * @throws TimeoutException If the cluster doesn't start within the lobbyTimeout
     */
    public Cluster join() throws Exception {
      return new Cluster(
          clusterName,
          expectedClusterMembers,
          coordinatorHost,
          coordinatorPort,
          coordinatorLobbyTimeout,
          memberPort,
          maxGrpcMessageSize);
    }
  }

  /** The name of this cluster */
  public final String clusterName;
  /** The number of cluster members expected */
  public final int expectedClusterMembers;
  /** The host of the coordinator */
  public final String coordinatorHost;
  /** The port the coordinator is running on */
  public final int coordinatorPort;
  /**
   * The timeout for the coordinator to notify this member that all members are present and the
   * cluster can start processing jobs
   */
  public final Duration coordinatorLobbyTimeout;
  /** The port this member is listening on for connections from peers */
  public final int memberPort;
  /** The unique id of this cluster member */
  public final ClusterMemberId clusterMemberId;
  /** The maximum allowed message size over the cluster's GRPC mesh connections */
  public final int maxGrpcMessageSize;

  private Cluster(
      String clusterName,
      int expectedClusterMembers,
      String coordinatorHost,
      int coordinatorPort,
      Duration coordinatorLobbyTimeout,
      int memberPort,
      int maxGrpcMessageSize)
      throws Exception {
    this.clusterName = clusterName;
    this.expectedClusterMembers = expectedClusterMembers;
    this.coordinatorHost = coordinatorHost;
    this.coordinatorPort = coordinatorPort;
    this.coordinatorLobbyTimeout = coordinatorLobbyTimeout;
    this.memberPort = memberPort;
    this.clusterMemberId = new ClusterMemberId();
    this.maxGrpcMessageSize = maxGrpcMessageSize;
    this.clusterMember = new ClusterMember(this);
  }

  @Override
  public void close() throws Exception {
    clusterMember.close();
  }

  /**
   * Is this cluster member the leader? Only one member will be the leader for the duration of a
   * cluster.
   *
   * @return true if this member is the leader, otherwise false
   */
  public boolean isLeader() {
    return clusterMember.isLeader();
  }

  /**
   * Wait for all cluster members to reach this line of code before continuing. By default, members
   * will race through jobs as fast as possible, only waiting when they need data from another
   * member. This can sometimes cause issues, so use this method to manually synchronise all cluster
   * members.
   */
  public void waitForAllMembers() {
    clusterMember.synchroniseValueFromLeader(
        () -> null, Serialisers.voidValues, expectedClusterMembers);
  }

  /**
   * Wait for all cluster members to reach this line of code, then run the given {@link Supplier} on
   * the cluster leader and replicate the result to all members. Useful for making distributed
   * decisions such as "should we continue this while loop?"
   *
   * @return The result of running the {@link Supplier} on the cluster leader, when all members have
   *     reached this line of code.
   */
  public <T> T waitAndReplicateToAllMembers(
      Supplier<T> supplyValueOnLeader, Serialiser<T> serialiser) {
    return clusterMember.synchroniseValueFromLeader(
        supplyValueOnLeader, serialiser, expectedClusterMembers);
  }

  public <T, IT extends Iterable<T>>
      DistributedIterator<List<T>> waitAndReplicateToAllMembersInBatches(
          int batchSize, Supplier<IT> supplyValuesOnLeader, Serialiser<T> serialiser) {
    return new DistributedIterator<>(
        this,
        () -> Iterables.partition(supplyValuesOnLeader.get(), batchSize),
        Serialisers.listEntries(serialiser));
  }

  /**
   * Add a DistributableDataReference to the accessible set for other cluster members to retrieve.
   * Expected to be called when persistence-type operations are running
   *
   * @see com.markosindustries.distroboy.core.operations.PersistToDisk
   * @see com.markosindustries.distroboy.core.operations.PersistToHeap
   * @param referenceId The identifier of the reference
   * @param distributableDataReference A distributable reference to some data
   * @param <I> The type of values contained in the referenced data
   */
  public <I> void addDistributableData(
      DataReferenceId referenceId, DistributableDataReference<I> distributableDataReference) {
    clusterMember.addDistributableData(referenceId, distributableDataReference);
  }

  private <I, O> List<DataReference> distributeReferencesRaw(
      DistributedOpSequence<I, DataReference, ? extends DataReferenceList<O>> opSequence)
      throws InterruptedException {
    final var dataReferencesResult = executeAsync(opSequence);

    if (dataReferencesResult.isClusterLeader()) {
      final var dataReferences = dataReferencesResult.getResult();

      // The members will all want remote data references from us
      log.debug(
          "Distributing {} of size {}",
          dataReferences.getClass().getSimpleName(),
          dataReferences.list().size());
      clusterMember.distributeDataReferences(dataReferences.list());
    }

    log.debug("Awaiting distributed references as {}", this.clusterMemberId);
    return clusterMember.awaitDistributedDataReferences();
  }

  public <I> DistributedOpSequence.Builder<SortRange, I, List<I>> redistributeOrderingBy(
      SortedDataReferenceList<I> dataReferences,
      Comparator<I> comparator,
      Serialiser<I> serialiser) {
    final var dataReferencesForSelf =
        dataReferences.list().stream()
            .filter(ref -> clusterMemberId.equals(ClusterMemberId.fromBytes(ref.getMemberId())))
            .toList();

    // Step 1 (local data sort) is done before we get here, because we demand a
    // SortedDataReferenceList

    // Step 2 - Each data set collects N equidistant samples of its data, where N is node count
    for (final DataReference dataReference : dataReferencesForSelf) {
      clusterMember.takeSortSamples(dataReference, expectedClusterMembers);
      clusterMember.<I>pushComparator(
          dataReference, comparator, serialiser, expectedClusterMembers);
    }

    // Step 3 - leader sorts all samples, takes N samples from them and broadcasts to all nodes
    final List<SortRange> clusterSortRanges =
        waitAndReplicateToAllMembers(
            () -> {
              final var clusterWideSampleCount =
                  dataReferences.list().size() * expectedClusterMembers;
              final var sortedClusterWideSamples =
                  dataReferences.list().stream()
                      .flatMap(
                          dataReference ->
                              IteratorTo.stream(
                                  serialiser.deserialiseIterator(
                                      clusterMember.retrieveSortSamplesFromMember(
                                          ClusterMemberId.fromBytes(dataReference.getMemberId()),
                                          dataReference))))
                      .sorted(comparator)
                      .iterator();
              final var rangeEndsInclusive =
                  DistributedSampleSort.takeEquidistantSamples(
                      clusterWideSampleCount, sortedClusterWideSamples, expectedClusterMembers);
              rangeEndsInclusive.sort(comparator);

              final var sortRanges = new ArrayList<SortRange>(expectedClusterMembers);
              final var sortRangeBuilder = SortRange.newBuilder();
              for (final I rangeEnd : rangeEndsInclusive) {
                final var rangeEndValue = serialiser.serialise(rangeEnd);
                sortRangeBuilder.setRangeEndInclusive(rangeEndValue);
                sortRanges.add(sortRangeBuilder.build());
                sortRangeBuilder.setRangeStartExclusive(rangeEndValue);
              }
              return sortRanges;
            },
            Serialisers.listEntries(Serialisers.protobufValues(SortRange::parseFrom)));

    // Step 4 - each node retrieves the portion of the total data in the range it was given
    final var sortRangesSource = new StaticDataSource<>(clusterSortRanges);
    return DistributedOpSequence.readFrom(sortRangesSource)
        .flatMap(
            sortRange -> {
              return new MergeSortingIteratorWithResources<>(
                  dataReferences.list().stream()
                      .map(
                          dataReference -> {
                            return serialiser.deserialiseIterator(
                                clusterMember.retrieveSortRangeFromMember(
                                    ClusterMemberId.fromBytes(dataReference.getMemberId()),
                                    DataReferenceSortRange.newBuilder()
                                        .setReference(dataReference)
                                        .setSortRange(sortRange)
                                        .build()));
                          })
                      .toList(),
                  comparator);
            });
  }

  /**
   * Give all nodes references to the data in its current form across the cluster. Data isn't
   * guaranteed to be stored, reiterable, or countable. For that, use {@link #persist}
   *
   * @param opSequence The set of operations to be performed before persisting the data
   * @param <I> The input type
   * @param <O> The output type
   * @return A list of references to the persisted data
   * @throws InterruptedException If interrupted while waiting for all references to be collected
   */
  public <I, O> DataReferenceList<O> distributeReferences(
      DistributedOpSequence<I, DataReference, DataReferenceList<O>> opSequence)
      throws InterruptedException {
    return new DataReferenceList<O>(distributeReferencesRaw(opSequence));
  }

  /**
   * Persist the data in its current form across the cluster, and give all nodes references
   *
   * @param opSequence The set of operations to be performed before persisting the data
   * @param <I> The input type
   * @param <O> The output type
   * @return A list of references to the persisted data
   * @throws InterruptedException If interrupted while waiting for all references to be collected
   */
  public <I, O> PersistedDataReferenceList<O> persist(
      DistributedOpSequence<I, DataReference, PersistedDataReferenceList<O>> opSequence)
      throws InterruptedException {
    return new PersistedDataReferenceList<O>(distributeReferencesRaw(opSequence));
  }

  /**
   * Persist and locally sort the data in its current form across the cluster, and give all nodes
   * references
   *
   * @param opSequence The set of operations to be performed before persisting the data
   * @param <I> The input type
   * @param <O> The output type
   * @return A list of references to the persisted data
   * @throws InterruptedException If interrupted while waiting for all references to be collected
   */
  public <I, O> SortedDataReferenceList<O> persistAndSort(
      DistributedOpSequence<I, DataReference, SortedDataReferenceList<O>> opSequence)
      throws InterruptedException {
    return new SortedDataReferenceList<O>(distributeReferencesRaw(opSequence));
  }

  /**
   * Given a list of references to persisted data, redistribute the results evenly across the
   * cluster for further processing.
   *
   * @param dataReferences The references to the data to redistribute
   * @param serialiser A serialiser for the data types persisted
   * @param <I> The persisted data type
   * @return A new DistributedOpSequence whose DataSource is the persisted data
   */
  public <I> DistributedOpSequence.Builder<DataReferenceRange, I, List<I>> redistributeEqually(
      PersistedDataReferenceList<I> dataReferences, Serialiser<I> serialiser) {
    return DistributedOpSequence.readFrom(new EvenlyRedistributedDataSource<I>(dataReferences))
        .flatMap(
            dataReference -> IteratorWithResources.from(retrieveRange(dataReference, serialiser)));
  }

  /**
   * Given a list of references to persisted data, redistribute the results across the cluster by
   * grouping them using a hashing function. For any single hash, all data which matches that hash
   * will be processed on a single node.
   *
   * @param dataReferences The references to the data to redistribute
   * @param classifier Given an input I, returns some object H which will be hashed
   * @param hasher A function which takes the output H of the classifier, and hashes it to a number
   *     in Integer space
   * @param partitions The number of partitions desired (ie: the modulus to use for the hashes)
   * @param serialiser A serialiser for the data types persisted
   * @param <I> The persisted data type
   * @param <H> The type the classifier will return for generating hashes
   * @return A new DistributedOpSequence whose DataSource is the persisted data
   */
  public <I, H>
      DistributedOpSequence.IteratorBuilder<Integer, I, Iterator<I>, List<Iterator<I>>>
          redistributeByHash(
              DataReferenceList<I> dataReferences,
              Function<I, H> classifier,
              Function<H, Integer> hasher,
              int partitions,
              Serialiser<I> serialiser) {
    // We're expecting a bunch of calls to ask for this data classified.
    // We know how many, and we know which ones are for this member
    final var dataReferencesForSelf =
        dataReferences.list().stream()
            .filter(ref -> clusterMemberId.equals(ClusterMemberId.fromBytes(ref.getMemberId())))
            .toList();
    for (final var dataReference : dataReferencesForSelf) {
      clusterMember.<I>pushHasher(
          dataReference, x -> hasher.apply(classifier.apply(x)), partitions);
    }

    final var hashesSource =
        new StaticDataSource<>(IntStream.range(0, partitions).boxed().toList());
    return DistributedOpSequence.readFrom(hashesSource) // start with a hash value per node
        .mapToIterators(
            hash ->
                (Iterator<I>)
                    IteratorWithResources.from(
                        retrieveByHash(dataReferences.list(), hash, partitions, serialiser)))
        .materialise(); // important that we materialise, otherwise not all GRPC retrieveByHash
    // calls will be made
  }

  /**
   * A convenience method which will call {@link #redistributeByHash} and then apply a groupBy
   * operation as the first operation on the newly generated DistributedOpSequence
   *
   * @param dataReferences The references to the data to redistribute
   * @param classifier Given an input I, returns a grouping key K, which will be hashed
   * @param hasher A function which takes the grouping key K, and hashes it to a number in Integer
   *     space
   * @param partitions The number of partitions desired (ie: the modulus to use for the hashes)
   * @param serialiser A serialiser for the data types persisted
   * @param <I> The persisted data type
   * @param <K> The type of the key to group by (and which will be hashed for redistribution)
   * @return A new DistributedOpSequence whose DataSource is the persisted data
   */
  public <I, K> DistributedOpSequence.HashMapBuilder<Integer, K, List<I>> redistributeAndGroupBy(
      DataReferenceList<I> dataReferences,
      Function<I, K> classifier,
      Function<K, Integer> hasher,
      int partitions,
      Serialiser<I> serialiser) {
    return redistributeByHash(dataReferences, classifier, hasher, partitions, serialiser)
        .groupBy(classifier::apply);
  }

  /**
   * Run the DistributedOpSequence on the cluster, and return a DistributedOpResult, which will
   * contain the collected results of all nodes' work on the leader. Care must be taken that this
   * result isn't too large, as it must fit into working memory on the cluster leader. Unlike
   * ${@link #execute(DistributedOpSequence)}, this method will <b>not</b> synchronise all cluster
   * members at the end of this operation - they will continue execution before any work arrives for
   * the distributed operation. It is easy to write race conditions with this, so prefer {@link
   * #execute(DistributedOpSequence)}. If you must use this - you can use {@link
   * #waitForAllMembers()} to ensure resources are still available when cluster members are
   * eventually asked to perform the operation sequence.
   *
   * @param opSequence The DistributedOpSequence the cluster of nodes should execute
   * @param <I> The input type of the operation sequence
   * @param <O> The output type of the operation sequence
   * @param <C> The collection type of the operation sequence.
   * @return The DistributedOpResult (which contains the collected results on the leader node)
   */
  public <I, O, C> AsyncDistributedOpResult<C> executeAsync(
      DistributedOpSequence<I, O, C> opSequence) {
    clusterMember.addJob(
        dataSourceRange -> {
          final var iterator = opSequence.getOperand().enumerateRangeForNode(dataSourceRange);
          return new MappingIteratorWithResources<>(
              iterator, opSequence.getSerialiser()::serialise);
        });

    if (!clusterMember.isLeader()) {
      return new AsyncDistributedOpResult.WorkerResult<>();
    }

    log.debug("{} - distributing to {}", clusterName, expectedClusterMembers);
    final var memberJobs = clusterMember.distributeDataSource(opSequence.getDataSource());

    log.debug("{} - collecting", clusterName);
    final var results =
        new FlatMappingIterator<>(
            memberJobs.iterator(),
            memberJob -> opSequence.getSerialiser().deserialiseIterator(memberJob.join()));

    return new AsyncDistributedOpResult.LeaderResult<>(opSequence.getOperand().collect(results));
  }

  /**
   * Run the DistributedOpSequence on the cluster, and return a DistributedOpResult, which will
   * contain the collected results of all nodes' work on the leader. Care must be taken that this
   * result isn't too large, as it must fit into working memory on the cluster leader. All cluster
   * members will block here until the results have been collected on the leader.
   *
   * @param opSequence The DistributedOpSequence the cluster of nodes should execute
   * @param <I> The input type of the operation sequence
   * @param <O> The output type of the operation sequence
   * @param <C> The collection type of the operation sequence.
   * @return The DistributedOpResult (which contains the collected results on the leader node)
   */
  public <I, O, C> DistributedOpResult<C> execute(DistributedOpSequence<I, O, C> opSequence) {
    return executeAsync(opSequence).waitForAllMembers(this);
  }

  private <O> Iterator<O> retrieveRange(
      DataReferenceRange dataReferenceRange, Serialiser<O> serialiser) {
    return serialiser.deserialiseIterator(
        clusterMember.retrieveRangeFromMember(
            ClusterMemberId.fromBytes(dataReferenceRange.getReference().getMemberId()),
            dataReferenceRange));
  }

  private <O> Iterator<O> retrieveByHash(
      List<DataReference> dataReferences, int hash, int modulo, Serialiser<O> serialiser) {
    // We materialise all of the GRPC iterators  up front so that each receiving cluster member
    // can iterate and hash a data reference exactly once, distributing based on hash%modulo
    final var valueIteratorsByMember =
        dataReferences.stream()
            .collect(
                groupingBy(
                    dataReference -> ClusterMemberId.fromBytes(dataReference.getMemberId()),
                    mapping(
                        dataReference -> {
                          final var memberId =
                              ClusterMemberId.fromBytes(dataReference.getMemberId());

                          return clusterMember.retrieveByHashFromMember(
                              memberId,
                              DataReferenceHashSpec.newBuilder()
                                  .setReference(dataReference)
                                  .setHash(hash)
                                  .setModulo(modulo)
                                  .build());
                        },
                        toUnmodifiableList())));

    return serialiser.deserialiseIterator(
        new FlatMappingIterator<>(
            valueIteratorsByMember.values().iterator(),
            valueIterators ->
                new FlatMappingIterator<>(valueIterators.iterator(), Function.identity())));
  }
}
