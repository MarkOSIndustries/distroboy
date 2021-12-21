package com.markosindustries.distroboy.core;

import com.markosindustries.distroboy.core.clustering.CoordinatorGrpcService;
import com.markosindustries.distroboy.core.clustering.ServerCallAddressInterceptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Methods for running the coordinator process used to start distroboy Clusters */
public final class Coordinator {
  private static final Logger log = LoggerFactory.getLogger(Coordinator.class);

  /**
   * Start the coordinator on a given port, and block the calling thread until the coordinator shuts
   * down (via SIGINT etc).
   *
   * @param port The port to run the coordinator on
   * @throws IOException If we can't start the coordinator RPC interface
   */
  public static void startAndBlockUntilShutdown(int port) throws IOException {
    runAsyncUntilShutdown(port).join();
  }

  /**
   * Start the coordinator on a given port, and return a CompletableFuture which will complete once
   * the coordinator shuts down (via SIGINT etc).
   *
   * @param port The port to run the coordinator on
   * @throws IOException If we can't start the coordinator RPC interface
   */
  public static CompletableFuture<Void> runAsyncUntilShutdown(int port) throws IOException {
    final var shutdownFuture = new CompletableFuture<Void>();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdownFuture.complete(null)));
    return Coordinator.runAsyncUntil(port, shutdownFuture);
  }

  /**
   * Start the coordinator on a given port, and return a CompletableFuture which will complete once
   * the coordinator shuts down (which will happen once the provided future completes).
   *
   * @param port The port to run the coordinator on
   * @param stop The future whose completion signals we should stop the coordinator
   * @throws IOException If we can't start the coordinator RPC interface
   */
  public static CompletableFuture<Void> runAsyncUntil(int port, CompletableFuture<Void> stop)
      throws IOException {
    final var server = runAsync(port);
    return stop.thenAcceptAsync(
        ignored -> {
          log.info("Shutting down");
          server.shutdown();
          try {
            server.awaitTermination();
          } catch (InterruptedException e) {
            // Just give up, it's shut down time anyway
          }
        });
  }

  /**
   * Start the coordinator on a given port, and return the GRPC Server instance.
   *
   * @param port The port to run the coordinator on
   * @throws IOException If we can't start the coordinator RPC interface
   */
  public static Server runAsync(int port) throws IOException {
    final var remoteAddressInterceptor = new ServerCallAddressInterceptor();
    final var server =
        ServerBuilder.forPort(port)
            .addService(new CoordinatorGrpcService(remoteAddressInterceptor))
            .intercept(remoteAddressInterceptor)
            .build()
            .start();
    log.info("Ready to accept connections");
    return server;
  }

  private Coordinator() {
    throw new UnsupportedOperationException();
  }
}
