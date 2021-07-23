package distroboy.core.clustering;

import com.google.protobuf.Empty;
import distroboy.schemas.ClusterMemberGrpc;
import distroboy.schemas.ClusterMemberIdentity;
import distroboy.schemas.DataReference;
import distroboy.schemas.DataReferenceHashSpec;
import distroboy.schemas.DataReferenceRange;
import distroboy.schemas.DataReferences;
import distroboy.schemas.DataSourceRange;
import distroboy.schemas.HostAndPort;
import distroboy.schemas.Value;
import io.grpc.ManagedChannelBuilder;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionToClusterMember {
  private static final Logger log = LoggerFactory.getLogger(ConnectionToClusterMember.class);

  private final ClusterMemberGrpc.ClusterMemberBlockingStub member;
  private final HostAndPort hostAndPort;

  public ConnectionToClusterMember(HostAndPort hostAndPort) {
    final var channel =
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

  public void disband() {
    member.disband(Empty.newBuilder().build());
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
