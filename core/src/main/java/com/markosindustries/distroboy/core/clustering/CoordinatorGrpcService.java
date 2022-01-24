package com.markosindustries.distroboy.core.clustering;

import static java.util.Objects.isNull;

import com.markosindustries.distroboy.schemas.ClusterMembers;
import com.markosindustries.distroboy.schemas.CoordinatorEvent;
import com.markosindustries.distroboy.schemas.CoordinatorGrpc;
import com.markosindustries.distroboy.schemas.HostAndPort;
import com.markosindustries.distroboy.schemas.JoinCluster;
import com.markosindustries.distroboy.schemas.MemberEvent;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of the Coordinator's GRPC interface */
public class CoordinatorGrpcService extends CoordinatorGrpc.CoordinatorImplBase {
  private static final Logger log = LoggerFactory.getLogger(CoordinatorGrpcService.class);
  private final RemoteAddressProvider remoteAddressProvider;

  /**
   * Implementation of the Coordinator's GRPC interface
   *
   * @param remoteAddressProvider Needed to determine cluster member IP addresses as they join a
   *     cluster
   */
  public CoordinatorGrpcService(RemoteAddressProvider remoteAddressProvider) {
    this.remoteAddressProvider = remoteAddressProvider;
  }

  private static class ClusterLobby {
    private final String clusterName;
    private final int expectedMembers;
    private final Map<HostAndPort, ServerCallStreamObserver<ClusterMembers>> members;

    public ClusterLobby(String clusterName, int expectedMembers) {
      this.clusterName = clusterName;
      this.expectedMembers = expectedMembers;
      this.members = new HashMap<>(expectedMembers);

      log.info("Lobby {} - started", clusterName);
    }

    public void add(
        HostAndPort member, ServerCallStreamObserver<ClusterMembers> serverCallStreamObserver) {
      members.put(member, serverCallStreamObserver);
      log.info("Lobby {} - member joined {}:{}", clusterName, member.getHost(), member.getPort());
    }

    public boolean tryStartCluster() {
      if (members.size() == expectedMembers) {
        boolean isLeader = true;
        HostAndPort leader = null;
        for (final var member : members.entrySet()) {
          if (isLeader) {
            leader = member.getKey();
          }
          final var observer = member.getValue();
          observer.onNext(
              ClusterMembers.newBuilder()
                  .setIsLeader(isLeader)
                  .setLeaderAddress(leader)
                  .addAllClusterMembers(members.keySet())
                  .build());
          observer.onCompleted();
          isLeader = false;
        }
        log.info("Lobby {} - cluster started", clusterName);
        return true;
      }
      log.info(
          "Lobby {} - awaiting more members {}/{}", clusterName, members.size(), expectedMembers);
      return false;
    }

    public void abort() {
      for (final var member : members.entrySet()) {
        final var observer = member.getValue();
        observer.onError(Status.ABORTED.asException());
      }
    }
  }

  private final ConcurrentMap<String, ClusterLobby> clusterLobbies = new ConcurrentHashMap<>();

  @Override
  public StreamObserver<MemberEvent> connect(StreamObserver<CoordinatorEvent> responseObserver) {
    return super.connect(responseObserver);
  }

  @Override
  public void joinCluster(JoinCluster request, StreamObserver<ClusterMembers> responseObserver) {
    try {
      final var serverCallStreamObserver =
          (ServerCallStreamObserver<ClusterMembers>) responseObserver;

      final var lobby =
          clusterLobbies.computeIfAbsent(
              request.getClusterName(),
              clusterName ->
                  new ClusterLobby(request.getClusterName(), request.getExpectedMembers()));
      synchronized (lobby) {
        if (lobby.expectedMembers != request.getExpectedMembers()) {
          log.error(
              "The lobby for {} expected {} members, not {}",
              request.getClusterName(),
              lobby.expectedMembers,
              request.getExpectedMembers());
          throw Status.INVALID_ARGUMENT.asException();
        }

        final var remoteAddressOfCurrentRequest =
            remoteAddressProvider.getRemoteAddressOfCurrentRequest();
        if (isNull(remoteAddressOfCurrentRequest)) {
          log.warn("Couldn't detect remote address... instructing member to retry");
          throw Status.UNAVAILABLE.asException();
        }

        final var observedMemberDetails =
            buildHostAndPort(remoteAddressOfCurrentRequest, request.getMemberPort());
        lobby.add(observedMemberDetails, serverCallStreamObserver);

        serverCallStreamObserver.setOnCancelHandler(
            () -> {
              log.error(
                  "{} - Some cluster members disconnected before the lobby was full",
                  request.getClusterName());
              clusterLobbies.remove(request.getClusterName());
              lobby.abort();
            });

        if (lobby.tryStartCluster()) {
          clusterLobbies.remove(request.getClusterName());
        }
      }
    } catch (StatusException e) {
      log.error("joinCluster request failed - ", e);
      responseObserver.onError(e);
    }
  }

  private static HostAndPort buildHostAndPort(
      final String remoteAddressOfCurrentRequest, int port) {
    var trimmedAddress = remoteAddressOfCurrentRequest;
    while (trimmedAddress.startsWith("/")) {
      trimmedAddress = trimmedAddress.substring(1);
    }
    final var portDelimiter = trimmedAddress.lastIndexOf(':');

    final var host = trimmedAddress.substring(0, portDelimiter);
    return HostAndPort.newBuilder().setHost(host).setPort(port).build();
  }
}
