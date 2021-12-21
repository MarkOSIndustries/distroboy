package com.markosindustries.distroboy.core.clustering;

import com.markosindustries.distroboy.schemas.ClusterMembers;
import com.markosindustries.distroboy.schemas.CoordinatorEvent;
import com.markosindustries.distroboy.schemas.CoordinatorGrpc;
import com.markosindustries.distroboy.schemas.JoinCluster;
import com.markosindustries.distroboy.schemas.MemberEvent;
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
  private final String coordinatorHost;
  private final int coordinatorPort;

  public ConnectionToCoordinator(String coordinatorHost, int coordinatorPort) {
    this.coordinatorHost = coordinatorHost;
    this.coordinatorPort = coordinatorPort;
    final var channel =
        ManagedChannelBuilder.forAddress(coordinatorHost, coordinatorPort).usePlaintext().build();
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
      } catch (ExecutionException | StatusRuntimeException ex) {
        Throwable cause = ex;
        while (cause instanceof ExecutionException) {
          cause = cause.getCause();
        }
        if (cause instanceof StatusRuntimeException) {
          StatusRuntimeException statusRuntimeException = (StatusRuntimeException) cause;
          if (CODES_TO_RETRY_JOIN_CLUSTER_ON.contains(
              statusRuntimeException.getStatus().getCode())) {
            log.info(
                "JoinCluster [{}:{}] failed with {}, retrying...",
                coordinatorHost,
                coordinatorPort,
                statusRuntimeException.getStatus().getCode());
            Thread.sleep(1000);
            continue;
          }
          log.error(
              "JoinCluster [{}:{}] failed with {}, failing",
              coordinatorHost,
              coordinatorPort,
              statusRuntimeException.getStatus().getCode());
          throw statusRuntimeException;
        }

        log.error("JoinCluster failed in an unexpected manner, failing", ex);
        throw ex;
      }
    }
  }
}
