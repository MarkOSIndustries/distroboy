package com.markosindustries.distroboy;

import com.markosindustries.distroboy.core.Cluster;
import com.markosindustries.distroboy.core.Coordinator;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DistroBoySingleProcess {
  private static final int COORDINATOR_PORT = 7070;
  private static final int MEMBER_PORTS_START_AT = 7071;
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
    INSTANCE.runInternal(jobName, workerThreads, job);
  }

  public void runInternal(final String jobName, final int workerThreads, Job job) {
    final var executor = Executors.newFixedThreadPool(workerThreads);
    try {
      CompletableFuture.allOf(
              IntStream.range(0, workerThreads)
                  .mapToObj(
                      threadIndex -> {
                        return CompletableFuture.runAsync(
                            () -> {
                              try (final var cluster =
                                  Cluster.newBuilder(jobName, workerThreads)
                                      .coordinator("localhost", COORDINATOR_PORT)
                                      .memberPort(MEMBER_PORTS_START_AT + threadIndex)
                                      .join()) {
                                job.startThread(cluster);
                              } catch (Exception e) {
                                // It'll shut down..
                              }
                            },
                            executor);
                      })
                  .collect(Collectors.toUnmodifiableList())
                  .toArray(CompletableFuture[]::new))
          .join();
    } finally {
      executor.shutdown();
    }
  }
}
