package com.markosindustries.distroboy.core.clustering;

import com.google.protobuf.Empty;
import com.markosindustries.distroboy.core.Cluster;
import com.markosindustries.distroboy.schemas.ClusterMemberGrpc;
import com.markosindustries.distroboy.schemas.ClusterMemberIdentity;
import com.markosindustries.distroboy.schemas.DataReference;
import com.markosindustries.distroboy.schemas.DataReferenceHashSpec;
import com.markosindustries.distroboy.schemas.DataReferenceRange;
import com.markosindustries.distroboy.schemas.DataReferenceSortRange;
import com.markosindustries.distroboy.schemas.DataReferences;
import com.markosindustries.distroboy.schemas.DataSourceRange;
import com.markosindustries.distroboy.schemas.HostAndPort;
import com.markosindustries.distroboy.schemas.SynchronisationPoint;
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
  ConnectionToClusterMember(final HostAndPort hostAndPort, final Cluster cluster) {
    this.channel =
        ManagedChannelBuilder.forAddress(hostAndPort.getHost(), hostAndPort.getPort())
            .usePlaintext()
            .maxInboundMessageSize(cluster.maxGrpcMessageSize)
            .build();
    this.member =
        ClusterMemberGrpc.newBlockingStub(channel)
            .withMaxOutboundMessageSize(cluster.maxGrpcMessageSize)
            .withMaxInboundMessageSize(cluster.maxGrpcMessageSize);
    this.hostAndPort = hostAndPort;
  }

  public ClusterMemberIdentity identify() {
    return member.identify(Empty.newBuilder().build());
  }

  public Iterator<Value> process(DataSourceRange dataSourceRange) {
    return member.process(dataSourceRange);
  }

  public void distribute(List<DataReference> clusterMemberDataReferences) {
    //noinspection ResultOfMethodCallIgnored
    member.distribute(
        DataReferences.newBuilder().addAllReferences(clusterMemberDataReferences).build());
  }

  public Iterator<Value> retrieveRange(DataReferenceRange dataReferenceRange) {
    return member.retrieveRange(dataReferenceRange);
  }

  public Iterator<Value> retrieveByHash(DataReferenceHashSpec dataReferenceHashSpec) {
    return member.retrieveByHash(dataReferenceHashSpec);
  }

  public Iterator<Value> retrieveSortSamples(DataReference dataReference) {
    return member.retrieveSortSamples(dataReference);
  }

  public Iterator<Value> retrieveSortRange(DataReferenceSortRange dataReferenceSortRange) {
    return member.retrieveSortRange(dataReferenceSortRange);
  }

  public HostAndPort getHostAndPort() {
    return hostAndPort;
  }

  public Optional<Value> synchronise(int index) {
    try {
      return Optional.of(
          member.synchronise(SynchronisationPoint.newBuilder().setIndex(index).build()));
    } catch (StatusRuntimeException sre) {
      if (sre.getStatus() == Status.NOT_FOUND) {
        return Optional.empty();
      }
      throw sre;
    }
  }

  public void forceDisband() {
    //noinspection ResultOfMethodCallIgnored
    member.forceDisband(Empty.newBuilder().build());
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
