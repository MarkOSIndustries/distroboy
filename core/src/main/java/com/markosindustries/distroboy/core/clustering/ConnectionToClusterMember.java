package com.markosindustries.distroboy.core.clustering;

import com.google.protobuf.Empty;
import com.markosindustries.distroboy.schemas.ClusterMemberGrpc;
import com.markosindustries.distroboy.schemas.ClusterMemberIdentity;
import com.markosindustries.distroboy.schemas.DataReference;
import com.markosindustries.distroboy.schemas.DataReferenceHashSpec;
import com.markosindustries.distroboy.schemas.DataReferenceRange;
import com.markosindustries.distroboy.schemas.DataReferences;
import com.markosindustries.distroboy.schemas.DataSourceRange;
import com.markosindustries.distroboy.schemas.HostAndPort;
import com.markosindustries.distroboy.schemas.Value;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Represents a connection to a cluster member */
class ConnectionToClusterMember implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(ConnectionToClusterMember.class);

  private final ClusterMemberGrpc.ClusterMemberBlockingStub member;
  private final HostAndPort hostAndPort;
  private final ManagedChannel channel;

  /** Represents a connection to a cluster member */
  ConnectionToClusterMember(HostAndPort hostAndPort) {
    this.channel =
        ManagedChannelBuilder.forAddress(hostAndPort.getHost(), hostAndPort.getPort())
            .usePlaintext()
            .build();
    this.member = ClusterMemberGrpc.newBlockingStub(channel);
    this.hostAndPort = hostAndPort;
  }

  public ClusterMemberIdentity identify() {
    return member.identify(Empty.newBuilder().build());
  }

  public Iterator<Value> process(DataSourceRange dataSourceRange) {
    return member.process(dataSourceRange);
  }

  public void distribute(List<DataReference> clusterMemberDataReferences) {
    member.distribute(
        DataReferences.newBuilder().addAllReferences(clusterMemberDataReferences).build());
  }

  public Iterator<Value> retrieveRange(DataReferenceRange dataReferenceRange) {
    return member.retrieveRange(dataReferenceRange);
  }

  public Iterator<Value> retrieveByHash(DataReferenceHashSpec dataReferenceHashSpec) {
    return member.retrieveByHash(dataReferenceHashSpec);
  }

  public HostAndPort getHostAndPort() {
    return hostAndPort;
  }

  public Optional<Value> synchronise() {
    try {
      return Optional.of(member.synchronise(Empty.newBuilder().build()));
    } catch (StatusRuntimeException sre) {
      if (sre.getStatus() == Status.NOT_FOUND) {
        return Optional.empty();
      }
      throw sre;
    }
  }

  @Override
  public void close() throws Exception {
    channel.shutdown();
    while (!channel.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
      log.debug("Awaiting shutdown of channel to Cluster peer");
    }
  }

  @Override
  public String toString() {
    return "ConnectionToClusterMember{"
        + "host="
        + hostAndPort.getHost()
        + ", port="
        + hostAndPort.getPort()
        + '}';
  }
}
