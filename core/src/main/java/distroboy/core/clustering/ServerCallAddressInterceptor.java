package distroboy.core.clustering;

import static java.util.Objects.nonNull;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

/** Provides the remote host address by intercepting GRPC headers */
public class ServerCallAddressInterceptor implements ServerInterceptor, RemoteAddressProvider {
  private final ThreadLocal<String> currentRequestClientAddress =
      ThreadLocal.withInitial(() -> null);

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    final var remoteAddress = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
    if (nonNull(remoteAddress)) {
      currentRequestClientAddress.set(remoteAddress.toString());
    } else {
      currentRequestClientAddress.set(null);
    }
    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(
        next.startCall(call, headers)) {};
  }

  @Override
  public String getRemoteAddressOfCurrentRequest() {
    return currentRequestClientAddress.get();
  }
}
