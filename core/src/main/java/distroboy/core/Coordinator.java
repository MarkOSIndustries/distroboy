package distroboy.core;

import distroboy.core.clustering.CoordinatorGrpcService;
import distroboy.core.clustering.ServerCallAddressInterceptor;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Coordinator {
  private static final Logger log = LoggerFactory.getLogger(Coordinator.class);

  public static void run(int port) throws IOException, InterruptedException {
    final var shutdownFuture = new CompletableFuture<Void>();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdownFuture.complete(null)));
    Coordinator.runUntil(port, shutdownFuture);
  }

  public static void runUntil(int port, CompletableFuture<Void> stop)
      throws IOException, InterruptedException {
    final var remoteAddressInterceptor = new ServerCallAddressInterceptor();
    final var server =
        ServerBuilder.forPort(port)
            .addService(new CoordinatorGrpcService(remoteAddressInterceptor))
            .intercept(remoteAddressInterceptor)
            .build()
            .start();
    log.info("Ready to accept connections");
    stop.join();
    server.shutdown();
    server.awaitTermination();
  }

  private Coordinator() {
    throw new UnsupportedOperationException();
  }
}
