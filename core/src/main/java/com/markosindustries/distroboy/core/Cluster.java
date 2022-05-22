package com.markosindustries.distroboy.core;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toUnmodifiableList;

import com.markosindustries.distroboy.core.clustering.ClusterMember;
import com.markosindustries.distroboy.core.clustering.ClusterMemberId;
import com.markosindustries.distroboy.core.clustering.DataReferenceId;
import com.markosindustries.distroboy.core.clustering.DistributableDataReference;
import com.markosindustries.distroboy.core.clustering.serialisation.Serialiser;
import com.markosindustries.distroboy.core.iterators.FlatMappingIterator;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.iterators.MappingIteratorWithResources;
import com.markosindustries.distroboy.core.operations.DistributedOpSequence;
import com.markosindustries.distroboy.core.operations.EvenlyRedistributedDataSource;
import com.markosindustries.distroboy.core.operations.StaticDataSource;
import com.markosindustries.distroboy.schemas.DataReference;
import com.markosindustries.distroboy.schemas.DataReferenceHashSpec;
import com.markosindustries.distroboy.schemas.DataReferenceRange;
import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
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
          memberPort);
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

  private Cluster(
      String clusterName,
      int expectedClusterMembers,
      String coordinatorHost,
      int coordinatorPort,
      Duration coordinatorLobbyTimeout,
      int memberPort)
      throws Exception {
    this.clusterName = clusterName;
    this.expectedClusterMembers = expectedClusterMembers;
    this.coordinatorHost = coordinatorHost;
    this.coordinatorPort = coordinatorPort;
    this.coordinatorLobbyTimeout = coordinatorLobbyTimeout;
    this.memberPort = memberPort;
    this.clusterMemberId = new ClusterMemberId();
    this.clusterMember = new ClusterMember(this);
  }

  @Override
  public void close() throws Exception {
    clusterMember.close();
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
    final var dataReferencesResult = execute(opSequence);

    if (dataReferencesResult.isClusterLeader()) {
      final var dataReferences = dataReferencesResult.getResult();

      // The members will all want remote data references from us
      clusterMember.distributeDataReferences(dataReferences.list());
    }

    return clusterMember.awaitDistributedDataReferences();
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
            .collect(toUnmodifiableList());
    for (final var dataReference : dataReferencesForSelf) {
      clusterMember.<I>pushHasher(
          dataReference, x -> hasher.apply(classifier.apply(x)), partitions);
    }

    final var hashesSource =
        new StaticDataSource<>(
            IntStream.range(0, partitions).boxed().collect(toUnmodifiableList()));
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
   * result isn't too large, as it must fit into working memory on the cluster leader.
   *
   * @param opSequence The DistributedOpSequence the cluster of nodes should execute
   * @param <I> The input type of the operation sequence
   * @param <O> The output type of the operation sequence
   * @param <C> The collection type of the operation sequence.
   * @return The DistributedOpResult (which contains the collected results on the leader node)
   */
  public <I, O, C> DistributedOpResult<C> execute(DistributedOpSequence<I, O, C> opSequence) {
    clusterMember.addJob(
        dataSourceRange -> {
          final var iterator = opSequence.getOperand().enumerateRangeForNode(dataSourceRange);
          return new MappingIteratorWithResources<>(
              iterator, opSequence.getSerialiser()::serialise);
        });

    if (!clusterMember.isLeader()) {
      return new DistributedOpResult.WorkerResult<>();
    }

    log.debug("{} - distributing to {}", clusterName, expectedClusterMembers);
    final var memberJobs = clusterMember.distributeDataSource(opSequence.getDataSource());

    log.debug("{} - collecting", clusterName);
    final var results =
        new FlatMappingIterator<>(
            memberJobs.iterator(),
            memberJob -> opSequence.getSerialiser().deserialiseIterator(memberJob.join()));

    return new DistributedOpResult.LeaderResult<>(opSequence.getOperand().collect(results));
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
