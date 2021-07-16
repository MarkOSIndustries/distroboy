package distroboy.core;

import static distroboy.core.DataSourceRanges.describeRange;
import static distroboy.core.DataSourceRanges.generateRanges;
import static distroboy.core.clustering.ClusterMemberId.uuidFromBytes;

import distroboy.core.clustering.ClusterMember;
import distroboy.core.clustering.serialisation.Serialiser;
import distroboy.core.operations.ClusterPersistedDataSource;
import distroboy.core.operations.DataSource;
import distroboy.core.operations.DistributedOpSequence;
import distroboy.core.operations.FlatMapIterator;
import distroboy.core.operations.MappingIterator;
import distroboy.schemas.DataReference;
import distroboy.schemas.DataReferenceRange;
import distroboy.schemas.Value;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
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
    private Duration coordinatorLobbyTimeout = Duration.ofSeconds(30);
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

  public <I, O> DataSource<DataReferenceRange> runPersist(
      DistributedOpSequence<I, O, List<DataReference>> opSequence) throws Exception {
    final var dataReferencesResult = execute(opSequence);

    if (dataReferencesResult.hasResult()) {
      final var dataReferences = dataReferencesResult.getResult();
      //
      //      final var memberIdentities = clusterMember.getMemberIdentities();
      //      final var clusterMemberDataReferences =
      //          dataReferences.stream()
      //              .map(
      //                  dataReference -> {
      //                    return DataReference.newBuilder()
      //                        .setReferenceId(dataReference.getReferenceId())
      //
      // .setNode(memberIdentities.get(uuidFromBytes(dataReference.getNodeId())))
      //                        .setCount(dataReference.getCount())
      //                        .build();
      //                  })
      //              .collect(toUnmodifiableList());

      // The members will all want remote data references from us
      clusterMember.distributeDataReferences(dataReferences);
    }

    return new ClusterPersistedDataSource(clusterMember.awaitDistributedDataReferences());
  }

  public <I, O, C> DistributedOpResult<C> runCollect(DistributedOpSequence<I, O, C> opSequence)
      throws Exception {
    return execute(opSequence);
  }

  public <I, O, C> DistributedOpResult<C> execute(DistributedOpSequence<I, O, C> opSequence) {
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

  public <O> Iterator<O> retrieve(DataReferenceRange dataReferenceRange, Serialiser<O> serialiser) {
    return serialiser.deserialiseIterator(
        clusterMember
            .getMember(uuidFromBytes(dataReferenceRange.getReference().getMemberId()))
            .retrieveRange(dataReferenceRange));
  }
}
