package distroboy.core.clustering;

import distroboy.schemas.ClusterMembers;
import distroboy.schemas.CoordinatorEvent;
import distroboy.schemas.CoordinatorGrpc;
import distroboy.schemas.JoinCluster;
import distroboy.schemas.MemberEvent;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionToCoordinator implements StreamObserver<CoordinatorEvent> {
  private static final Logger log = LoggerFactory.getLogger(ConnectionToCoordinator.class);

  private final CoordinatorGrpc.CoordinatorFutureStub coordinator;
  private final StreamObserver<MemberEvent> coordinatorStream;

  public ConnectionToCoordinator(String host, int port) {
    final var channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    this.coordinator = CoordinatorGrpc.newFutureStub(channel);
    this.coordinatorStream = CoordinatorGrpc.newStub(channel).connect(this);
  }

  @Override
  public void onNext(CoordinatorEvent value) {
    switch (value.getCoordinatorEventOneofCase()) {
      case CLUSTER_MEMBERS -> {
        // TODO: this is probably not needed
        //  keeping the bi-direction stream around
        //  to implement observation type stuff
      }
    }
  }

  @Override
  public void onError(Throwable t) {}

  @Override
  public void onCompleted() {}

  private static final Set<Status.Code> CODES_TO_RETRY_JOIN_CLUSTER_ON =
      Set.of(Status.Code.UNAVAILABLE, Status.Code.ABORTED);

  public ClusterMembers joinCluster(
      String clusterName, int port, int expectedMembers, Duration timeout)
      throws ExecutionException, InterruptedException, TimeoutException {
    while (true) {
      try {
        JoinCluster joinCluster =
            JoinCluster.newBuilder()
                .setClusterName(clusterName)
                .setMemberPort(port)
                .setExpectedMembers(expectedMembers)
                .build();

        //    coordinatorStream.onNext(MemberEvent.newBuilder().setJoinJob(joinJob).build());
        return coordinator.joinCluster(joinCluster).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      } catch (StatusRuntimeException statusRuntimeException) {
        if (CODES_TO_RETRY_JOIN_CLUSTER_ON.contains(statusRuntimeException.getStatus().getCode())) {
          log.info(
              "JoinCluster failed with {}, retrying...",
              statusRuntimeException.getStatus().getCode());
          continue;
        }
        log.error(
            "JoinCluster failed with {}, failing", statusRuntimeException.getStatus().getCode());
        throw statusRuntimeException;
      }
    }
  }
}
