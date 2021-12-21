package com.markosindustries.distroboy.core;

import static com.markosindustries.distroboy.core.DataSourceRanges.describeRange;
import static com.markosindustries.distroboy.core.DataSourceRanges.generateRanges;
import static com.markosindustries.distroboy.core.clustering.ClusterMemberId.uuidFromBytes;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toUnmodifiableList;

import com.markosindustries.distroboy.core.clustering.ClusterMember;
import com.markosindustries.distroboy.core.clustering.ClusterMemberId;
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
import com.markosindustries.distroboy.schemas.Value;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
      coordinatorLobbyTimeout = timeout;
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
    public Cluster join()
        throws IOException, ExecutionException, InterruptedException, TimeoutException {
      return new Cluster(
          clusterName,
          expectedClusterMembers,
          coordinatorHost,
          coordinatorPort,
          coordinatorLobbyTimeout,
          memberPort);
    }
  }

  public final String clusterName;
  public final int expectedClusterMembers;
  public final String coordinatorHost;
  public final int coordinatorPort;
  public final Duration coordinatorLobbyTimeout;
  public final int memberPort;

  private Cluster(
      String clusterName,
      int expectedClusterMembers,
      String coordinatorHost,
      int coordinatorPort,
      Duration coordinatorLobbyTimeout,
      int memberPort)
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    this.clusterName = clusterName;
    this.expectedClusterMembers = expectedClusterMembers;
    this.coordinatorHost = coordinatorHost;
    this.coordinatorPort = coordinatorPort;
    this.coordinatorLobbyTimeout = coordinatorLobbyTimeout;
    this.memberPort = memberPort;
    this.clusterMember = new ClusterMember(this);
  }

  @Override
  public void close() throws Exception {
    clusterMember.close();
  }

  /**
   * Persist the data in its current form across the cluster
   *
   * @param opSequence The set of operations to be performed before persisting the data
   * @param <I> The input type
   * @param <O> The output type
   * @return A list of references to the persisted data
   * @throws InterruptedException If interrupted while waiting for all references to be collected
   */
  public <I, O> List<DataReference> persist(
      DistributedOpSequence<I, O, List<DataReference>> opSequence) throws InterruptedException {
    final var dataReferencesResult = execute(opSequence);

    if (dataReferencesResult.isClusterLeader()) {
      final var dataReferences = dataReferencesResult.getResult();

      // The members will all want remote data references from us
      clusterMember.distributeDataReferences(dataReferences);
    }

    return clusterMember.awaitDistributedDataReferences();
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
      List<DataReference> dataReferences, Serialiser<I> serialiser) {
    return DistributedOpSequence.readFrom(new EvenlyRedistributedDataSource(dataReferences))
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
      DistributedOpSequence.IteratorBuilder<
              Integer, I, IteratorWithResources<I>, List<IteratorWithResources<I>>>
          redistributeByHash(
              List<DataReference> dataReferences,
              Function<I, H> classifier,
              Function<H, Integer> hasher,
              int partitions,
              Serialiser<I> serialiser) {
    // We're expecting a bunch of calls to ask for this data classified.
    // We know how many, and we know which ones are for this member
    final var dataReferencesForSelf =
        dataReferences.stream()
            .filter(ref -> ClusterMemberId.self.equals(uuidFromBytes(ref.getMemberId())))
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
                IteratorWithResources.from(
                    retrieveByHash(dataReferences, hash, partitions, serialiser)))
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
      List<DataReference> dataReferences,
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

    // TODO: Move this stuff into ClusterMember maybe?;
    final var dataSourceRanges =
        generateRanges(opSequence.getDataSource().countOfFullSet(), expectedClusterMembers);
    final var memberJobs =
        new ArrayList<CompletableFuture<Iterator<Value>>>(dataSourceRanges.length);
    for (int i = 0; i < dataSourceRanges.length; i++) {
      log.debug("{} - distributing {}", clusterName, describeRange(dataSourceRanges[i]));
      final var member = clusterMember.getMembers()[i];
      final var range = dataSourceRanges[i];
      memberJobs.add(CompletableFuture.supplyAsync(() -> member.process(range)));
    }

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
        clusterMember
            .getMember(uuidFromBytes(dataReferenceRange.getReference().getMemberId()))
            .retrieveRange(dataReferenceRange));
  }

  private <O> Iterator<O> retrieveByHash(
      List<DataReference> dataReferences, int hash, int modulo, Serialiser<O> serialiser) {
    // We materialise all of the GRPC iterators  up front so that each receiving cluster member
    // can iterate and hash a data reference exactly once, distributing based on hash%modulo
    final var valueIteratorsByMember =
        dataReferences.stream()
            .collect(
                groupingBy(
                    dataReference -> uuidFromBytes(dataReference.getMemberId()),
                    mapping(
                        dataReference -> {
                          final var memberId = uuidFromBytes(dataReference.getMemberId());
                          final var member = clusterMember.getMember(memberId);

                          return member.retrieveByHash(
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
