package com.markosindustries.distroboy;

import com.markosindustries.distroboy.core.Cluster;
import com.markosindustries.distroboy.core.Coordinator;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistroBoySingleProcess {
  private static final Logger log = LoggerFactory.getLogger(DistroBoySingleProcess.class);

  private static final int COORDINATOR_PORT = 7170;
  private static final AtomicInteger MEMBER_PORTS = new AtomicInteger(7171);
  private static final DistroBoySingleProcess INSTANCE = new DistroBoySingleProcess();

  private DistroBoySingleProcess() {
    try {
      Coordinator.runAsyncUntilShutdown(COORDINATOR_PORT);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public interface Job {
    void startThread(Cluster cluster) throws Exception;
  }

  public static void run(final String jobName, final int workerThreads, Job job) throws Exception {
    INSTANCE.runInternal(jobName, workerThreads, workerThreads, Function.identity(), job);
  }

  public static void run(
      final String jobName,
      final int workerThreads,
      final Function<Cluster.Builder, Cluster.Builder> configureCluster,
      Job job)
      throws Exception {
    INSTANCE.runInternal(jobName, workerThreads, workerThreads, configureCluster, job);
  }

  public static void run(
      final String jobName, final int workerThreads, final int expectedWorkers, Job job)
      throws Exception {
    INSTANCE.runInternal(jobName, workerThreads, expectedWorkers, Function.identity(), job);
  }

  public void runInternal(
      final String jobName,
      final int workerThreads,
      final int expectedWorkers,
      final Function<Cluster.Builder, Cluster.Builder> configureCluster,
      Job job) {
    final var executor =
        Executors.newFixedThreadPool(
            workerThreads,
            new ThreadFactory() {
              private static final AtomicInteger ai = new AtomicInteger();

              @Override
              public Thread newThread(final Runnable r) {
                final var thread = new Thread(r, jobName + "-" + ai.incrementAndGet());
                thread.setDaemon(false);
                return thread;
              }
            });
    try {
      CompletableFuture.allOf(
              IntStream.range(0, workerThreads)
                  .mapToObj(
                      threadIndex -> {
                        return CompletableFuture.runAsync(
                            () -> {
                              try (final var cluster =
                                  configureCluster
                                      .apply(Cluster.newBuilder(jobName, expectedWorkers))
                                      .coordinator("localhost", COORDINATOR_PORT)
                                      .memberPort(MEMBER_PORTS.getAndIncrement())
                                      .lobbyTimeout(Duration.ofSeconds(1))
                                      .join()) {
                                Thread.currentThread().setName(cluster.clusterMemberId.toString());
                                job.startThread(cluster);
                              } catch (Exception e) {
                                // It'll shut down..
                                log.error("Node threw, cluster will shut down", e);
                                throw new RuntimeException("Node " + threadIndex + " threw", e);
                              }
                            },
                            executor);
                      })
                  .toArray(CompletableFuture[]::new))
          .join();
    } finally {
      executor.shutdown();
    }
  }
}
