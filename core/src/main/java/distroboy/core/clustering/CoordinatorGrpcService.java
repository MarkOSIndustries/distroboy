package distroboy.core.clustering;

import distroboy.schemas.ClusterMembers;
import distroboy.schemas.CoordinatorEvent;
import distroboy.schemas.CoordinatorGrpc;
import distroboy.schemas.HostAndPort;
import distroboy.schemas.JoinCluster;
import distroboy.schemas.MemberEvent;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.SyncFailedException;
import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoordinatorGrpcService extends CoordinatorGrpc.CoordinatorImplBase {
  private static final Logger log = LoggerFactory.getLogger(CoordinatorGrpcService.class);
  private final RemoteAddressProvider remoteAddressProvider;

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
        observer.onError(
            new SyncFailedException("Some cluster members disconnected before the lobby was full"));
      }
    }
  }

  private final Map<String, ClusterLobby> clusterLobbies = new HashMap<>();

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
      if (lobby.expectedMembers != request.getExpectedMembers()) {
        throw new InputMismatchException(
            "The lobby for "
                + request.getClusterName()
                + " expects "
                + lobby.expectedMembers
                + " members, not "
                + request.getExpectedMembers());
      }

      final var observedMemberDetails =
          buildHostAndPort(
              remoteAddressProvider.getRemoteAddressOfCurrentRequest(), request.getMemberPort());
      lobby.add(observedMemberDetails, serverCallStreamObserver);

      serverCallStreamObserver.setOnCancelHandler(
          () -> {
            log.error(
                "{} - Some cluster members disconnected before the lobby was full",
                request.getClusterName());
            lobby.abort();
            clusterLobbies.remove(request.getClusterName());
          });

      if (lobby.tryStartCluster()) {
        clusterLobbies.remove(request.getClusterName());
      }
    } catch (InputMismatchException e) {
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
