package distroboy.core;

import distroboy.core.clustering.CoordinatorGrpcService;
import distroboy.core.clustering.ServerCallAddressInterceptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Coordinator {
  private static final Logger log = LoggerFactory.getLogger(Coordinator.class);

  public static void startAndBlockUntilShutdown(int port) throws IOException {
    runAsyncUntilShutdown(port).join();
  }

  public static CompletableFuture<Void> runAsyncUntilShutdown(int port) throws IOException {
    final var shutdownFuture = new CompletableFuture<Void>();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdownFuture.complete(null)));
    return Coordinator.runAsyncUntil(port, shutdownFuture);
  }

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
