package distroboy.core.clustering;

import static distroboy.core.DataSourceRanges.describeRange;
import static distroboy.core.clustering.ClusterMemberId.uuidAsBytes;
import static distroboy.core.clustering.ClusterMemberId.uuidFromBytes;
import static java.util.stream.Collectors.toUnmodifiableMap;

import com.google.protobuf.Empty;
import distroboy.core.Cluster;
import distroboy.schemas.ClusterMemberGrpc;
import distroboy.schemas.ClusterMemberIdentity;
import distroboy.schemas.ClusterMembers;
import distroboy.schemas.DataReference;
import distroboy.schemas.DataReferenceRange;
import distroboy.schemas.DataReferences;
import distroboy.schemas.DataSourceRange;
import distroboy.schemas.Value;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterMember extends ClusterMemberGrpc.ClusterMemberImplBase
    implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(ClusterMember.class);
  private final BlockingQueue<Job> jobs;
  private final AtomicBoolean disbanded = new AtomicBoolean(false);
  private final Object disbandedLock = new Object();
  private final AtomicReference<DataReferences> distributedReferences = new AtomicReference<>(null);
  private final Object distributedReferencesLock = new Object();
  private final Server memberServer;
  private final ConnectionToClusterMember[] members;
  private final boolean isLeader;
  private final Map<UUID, ConnectionToClusterMember> memberIdentities;

  public ClusterMember(Cluster cluster)
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    this.jobs = new LinkedBlockingQueue<>();
    this.memberServer = ServerBuilder.forPort(cluster.memberPort).addService(this).build().start();

    try {
      log.debug("{} - joining lobby", cluster.clusterName);
      final var coordinator =
          new ConnectionToCoordinator(cluster.coordinatorHost, cluster.coordinatorPort);
      ClusterMembers clusterMembers =
          coordinator.joinCluster(
              cluster.clusterName,
              cluster.memberPort,
              cluster.expectedClusterMembers,
              cluster.coordinatorLobbyTimeout);
      log.debug(
          "{} - cluster started, isLeader={}", cluster.clusterName, clusterMembers.getIsLeader());
      this.members =
          clusterMembers.getClusterMembersList().stream()
              .map(ConnectionToClusterMember::new)
              .toArray(ConnectionToClusterMember[]::new);
      this.isLeader = clusterMembers.getIsLeader();
      log.debug("{} - connected to {} workers", cluster.clusterName, this.members.length);
      this.memberIdentities =
          Arrays.stream(members)
              .collect(
                  toUnmodifiableMap(
                      worker -> uuidFromBytes(worker.identify().getNodeId()), Function.identity()));
    } catch (Throwable t) {
      memberServer.shutdown();
      throw t;
    }
  }

  public boolean isLeader() {
    return isLeader;
  }

  public ConnectionToClusterMember[] getMembers() {
    return members;
  }

  public <O> ConnectionToClusterMember getMember(UUID memberId) {
    return memberIdentities.get(memberId);
  }

  @Override
  public void identify(Empty request, StreamObserver<ClusterMemberIdentity> responseObserver) {
    responseObserver.onNext(
        ClusterMemberIdentity.newBuilder().setNodeId(uuidAsBytes(ClusterMemberId.self)).build());
    responseObserver.onCompleted();
  }

  @Override
  public void process(DataSourceRange dataSourceRange, StreamObserver<Value> responseObserver) {
    log.debug("Starting job {}", describeRange(dataSourceRange));
    Job job = null;
    while (job == null) {
      try {
        job = jobs.poll(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        // Ignore, just try again
      }
    }

    log.debug("Responding to job {}", describeRange(dataSourceRange));
    Iterator<Value> iterator = job.execute(dataSourceRange);
    // TODO: backpressure
    while (iterator.hasNext()) {
      final var value = iterator.next();
      responseObserver.onNext(value);
    }
    responseObserver.onCompleted();
    log.debug("Finished job {}", describeRange(dataSourceRange));
  }

  @Override
  public void distribute(DataReferences request, StreamObserver<Empty> responseObserver) {
    distributedReferences.set(request);
    synchronized (distributedReferencesLock) {
      distributedReferencesLock.notify();
    }
    responseObserver.onNext(Empty.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void retrieveRange(DataReferenceRange request, StreamObserver<Value> responseObserver) {
    final var memberId = uuidFromBytes(request.getReference().getMemberId());
    if (!ClusterMemberId.self.equals(memberId)) {
      log.error(
          "Wrong memberId for data range retrieval, this is probably broken -- {} {}",
          memberId,
          ClusterMemberId.self);
    }

    // TODO: backpressure
    final var iterator =
        PersistedData.storedReferences
            .get(uuidFromBytes(request.getReference().getReferenceId()))
            .get();
    while (iterator.hasNext()) {
      final var value = iterator.next();
      responseObserver.onNext(value);
    }
    responseObserver.onCompleted();
  }

  @Override
  public void disband(Empty request, StreamObserver<Empty> responseObserver) {
    log.debug("Received disband");
    synchronized (disbandedLock) {
      disbanded.set(true);
      disbandedLock.notify();
    }
    log.debug("Disbanded");
    responseObserver.onNext(Empty.newBuilder().build());
    responseObserver.onCompleted();
  }

  public void addJob(Job job) {
    jobs.add(job);
  }

  public void distributeDataReferences(List<DataReference> remoteDataReferences) {
    for (ConnectionToClusterMember member : members) {
      member.distribute(remoteDataReferences);
    }
  }

  public List<DataReference> awaitDistributedDataReferences() throws InterruptedException {
    synchronized (distributedReferencesLock) {
      while (distributedReferences.get() == null) {
        distributedReferencesLock.wait();
      }
    }
    return distributedReferences.get().getReferencesList();
  }

  @Override
  public void close() throws Exception {
    log.debug("Closing");
    if (isLeader) {
      log.debug("Closing - sending disbands");
      for (ConnectionToClusterMember member : members) {
        log.debug("Closing - sending disband to {}", member);
        member.disband();
      }
    }
    log.debug("Closing - awaiting disband");
    synchronized (disbandedLock) {
      while (!disbanded.get()) {
        disbandedLock.wait();
      }
    }
    log.debug("Closing - disbanded");
    try {
      memberServer.shutdown();
      memberServer.awaitTermination();
    } catch (Throwable t) {
      log.error("Couldn't shut down cluster member server - potential resource leakage", t);
      throw t;
    }
  }
}
