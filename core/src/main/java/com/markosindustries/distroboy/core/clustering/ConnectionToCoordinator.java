package com.markosindustries.distroboy.core.clustering;

import com.markosindustries.distroboy.schemas.ClusterMembers;
import com.markosindustries.distroboy.schemas.CoordinatorEvent;
import com.markosindustries.distroboy.schemas.CoordinatorGrpc;
import com.markosindustries.distroboy.schemas.JoinCluster;
import io.grpc.ManagedChannel;
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

/** Represents a connection to the coordinator */
class ConnectionToCoordinator implements StreamObserver<CoordinatorEvent>, AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(ConnectionToCoordinator.class);

  private final CoordinatorGrpc.CoordinatorFutureStub coordinator;
  private final String coordinatorHost;
  private final int coordinatorPort;
  private final ManagedChannel channel;

  /** Represents a connection to the coordinator */
  ConnectionToCoordinator(
      String coordinatorHost, int coordinatorPort, final Duration lobbyTimeout) {
    this.coordinatorHost = coordinatorHost;
    this.coordinatorPort = coordinatorPort;
    this.channel =
        ManagedChannelBuilder.forAddress(coordinatorHost, coordinatorPort).usePlaintext().build();
    this.coordinator =
        CoordinatorGrpc.newFutureStub(channel)
            .withDeadlineAfter(lobbyTimeout.toMillis(), TimeUnit.MILLISECONDS);
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

  public ClusterMembers joinCluster(String clusterName, int port, int expectedMembers)
      throws ExecutionException, InterruptedException, TimeoutException {
    while (true) {
      try {
        JoinCluster joinCluster =
            JoinCluster.newBuilder()
                .setClusterName(clusterName)
                .setMemberPort(port)
                .setExpectedMembers(expectedMembers)
                .build();
        return coordinator.joinCluster(joinCluster).get();
      } catch (ExecutionException | StatusRuntimeException ex) {
        Throwable cause = ex;
        while (cause instanceof ExecutionException) {
          cause = cause.getCause();
        }
        if (cause instanceof final StatusRuntimeException statusRuntimeException) {
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
              "JoinCluster [{}:{}] failed with {}, giving up",
              coordinatorHost,
              coordinatorPort,
              statusRuntimeException.getStatus().getCode());
          throw statusRuntimeException;
        }

        log.error("JoinCluster failed in an unexpected manner, giving up", ex);
        throw ex;
      }
    }
  }

  @Override
  public void close() throws Exception {
    channel.shutdown();
    while (!channel.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
      log.debug("Awaiting shutdown of channel to Coordinator");
    }
  }
}
