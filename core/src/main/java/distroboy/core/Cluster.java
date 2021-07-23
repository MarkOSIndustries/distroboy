package distroboy.core;

import static distroboy.core.DataSourceRanges.describeRange;
import static distroboy.core.DataSourceRanges.generateRanges;
import static distroboy.core.clustering.ClusterMemberId.uuidFromBytes;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toUnmodifiableList;

import distroboy.core.clustering.ClusterMember;
import distroboy.core.clustering.ClusterMemberId;
import distroboy.core.clustering.serialisation.Serialiser;
import distroboy.core.operations.DistributedOpSequence;
import distroboy.core.operations.EvenlyRedistributedDataSource;
import distroboy.core.operations.FlatMapIterator;
import distroboy.core.operations.MappingIterator;
import distroboy.core.operations.StaticDataSource;
import distroboy.schemas.DataReference;
import distroboy.schemas.DataReferenceHashSpec;
import distroboy.schemas.DataReferenceRange;
import distroboy.schemas.Value;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Cluster implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(Cluster.class);
  private final ClusterMember clusterMember;

  public static Builder newBuilder(String clusterName, int expectedClusterMembers) {
    return new Builder(clusterName, expectedClusterMembers);
  }

  public static class Builder {
    private final String clusterName;
    private final int expectedClusterMembers;
    private String coordinatorHost = "localhost";
    private int coordinatorPort = 7070;
    private Duration coordinatorLobbyTimeout = Duration.ofMinutes(5);
    private int memberPort = 7071;

    public Builder(String clusterName, int expectedClusterMembers) {
      this.clusterName = clusterName;
      this.expectedClusterMembers = expectedClusterMembers;
    }

    public Builder coordinator(String host, int port) {
      coordinatorHost = host;
      coordinatorPort = port;
      return this;
    }

    public Builder lobbyTimeout(Duration timeout) {
      coordinatorLobbyTimeout = timeout;
      return this;
    }

    public Builder memberPort(int port) {
      memberPort = port;
      return this;
    }

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

  public <I, O> List<DataReference> persist(
      DistributedOpSequence<I, O, List<DataReference>> opSequence) throws Exception {
    final var dataReferencesResult = execute(opSequence);

    if (dataReferencesResult.hasResult()) {
      final var dataReferences = dataReferencesResult.getResult();

      // The members will all want remote data references from us
      clusterMember.distributeDataReferences(dataReferences);
    }

    return clusterMember.awaitDistributedDataReferences();
  }

  public <I> DistributedOpSequence.Builder<DataReferenceRange, I, List<I>> redistributeEqually(
      List<DataReference> dataReferences, Serialiser<I> serialiser) {
    return DistributedOpSequence.readFrom(new EvenlyRedistributedDataSource(dataReferences))
        .flatMap(dataReference -> retrieveRange(dataReference, serialiser));
  }

  public <I, H>
      DistributedOpSequence.IteratorBuilder<Integer, I, Iterator<I>, List<Iterator<I>>>
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
        .mapToIterators(hash -> retrieveByHash(dataReferences, hash, partitions, serialiser))
        .materialise(); // important that we materialise, otherwise not all GRPC retrieveByHash
    // calls will be made
  }

  public <I, K>
      DistributedOpSequence.Builder<Integer, Map.Entry<K, List<I>>, Map<K, List<I>>>
          redistributeAndGroupBy(
              List<DataReference> dataReferences,
              Function<I, K> classifier,
              Function<K, Integer> hasher,
              int partitions,
              Serialiser<I> serialiser) {
    return redistributeByHash(dataReferences, classifier, hasher, partitions, serialiser)
        .groupBy(classifier::apply);
  }

  public <I, O, C> DistributedOpResult<C> collect(DistributedOpSequence<I, O, C> opSequence)
      throws Exception {
    return execute(opSequence);
  }

  private <I, O, C> DistributedOpResult<C> execute(DistributedOpSequence<I, O, C> opSequence) {
    clusterMember.addJob(
        dataSourceRange -> {
          final var iterator = opSequence.getOperand().enumerateRangeForNode(dataSourceRange);
          return new MappingIterator<>(iterator, opSequence.getSerialiser()::serialise);
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
        new FlatMapIterator<>(
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
        new FlatMapIterator<>(
            valueIteratorsByMember.values().iterator(),
            valueIterators ->
                new FlatMapIterator<>(valueIterators.iterator(), Function.identity())));
  }
}
