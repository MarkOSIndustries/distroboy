package com.markosindustries.distroboy.core.clustering;

import static java.util.Objects.nonNull;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

/** Provides the remote host address by intercepting GRPC headers */
public class ServerCallAddressInterceptor implements ServerInterceptor, RemoteAddressProvider {
  private static final Context.Key<String> CLIENT_ADDRESS = Context.key("client-address");

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    final var remoteAddress = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
    if (nonNull(remoteAddress)) {
      return Contexts.interceptCall(
          Context.current().withValue(CLIENT_ADDRESS, remoteAddress.toString()),
          call,
          headers,
          next);
    }

    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(
        next.startCall(call, headers)) {};
  }

  @Override
  public String getRemoteAddressOfCurrentRequest() {
    return CLIENT_ADDRESS.get(Context.current());
  }
}
