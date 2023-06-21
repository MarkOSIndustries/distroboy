package com.markosindustries.distroboy;

import com.markosindustries.distroboy.core.Cluster;
import com.markosindustries.distroboy.core.Coordinator;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistroBoySingleProcess {
  private static final Logger log = LoggerFactory.getLogger(DistroBoySingleProcess.class);

  private static final int COORDINATOR_PORT = 7170;
  private static final int MEMBER_PORTS_START_AT = 7171;
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
                                  Cluster.newBuilder(jobName, workerThreads)
                                      .coordinator("localhost", COORDINATOR_PORT)
                                      .memberPort(MEMBER_PORTS_START_AT + threadIndex)
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
