package com.markosindustries.distroboy.core.clustering;

/** Provides the remote address for an in-flight GRPC request */
public interface RemoteAddressProvider {
  /**
   * Get the remote address of an in-flight GRPC request
   *
   * @return The remote address
   */
  String getRemoteAddressOfCurrentRequest();
}
