package com.markosindustries.distroboy.core.clustering;

import static com.markosindustries.distroboy.core.DataSourceRanges.describeRange;
import static com.markosindustries.distroboy.core.DataSourceRanges.generateRanges;
import static java.util.stream.Collectors.toUnmodifiableMap;

import com.google.protobuf.Empty;
import com.markosindustries.distroboy.core.Cluster;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.operations.DataSource;
import com.markosindustries.distroboy.schemas.ClusterMemberGrpc;
import com.markosindustries.distroboy.schemas.ClusterMemberIdentity;
import com.markosindustries.distroboy.schemas.ClusterMembers;
import com.markosindustries.distroboy.schemas.DataReference;
import com.markosindustries.distroboy.schemas.DataReferenceHashSpec;
import com.markosindustries.distroboy.schemas.DataReferenceRange;
import com.markosindustries.distroboy.schemas.DataReferences;
import com.markosindustries.distroboy.schemas.DataSourceRange;
import com.markosindustries.distroboy.schemas.Value;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a ClusterMember's capabilities and responsibilities Responsible for cluster
 * startup, answering queries from other members, and delegating work and queries to other cluster
 * members.
 */
public class ClusterMember extends ClusterMemberGrpc.ClusterMemberImplBase
    implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(ClusterMember.class);
  private final BlockingQueue<Job> jobs;
  private final ConcurrentMap<DataReferenceId, BlockingQueue<HashedDataReference<?>>>
      dataReferenceHashers;
  private final Object classifiersLock = new Object();
  private final AtomicBoolean disbanded = new AtomicBoolean(false);
  private final Object disbandedLock = new Object();
  private final AtomicReference<DataReferences> distributedReferences = new AtomicReference<>(null);
  private final Object distributedReferencesLock = new Object();
  private final Server memberServer;
  private final ConnectionToClusterMember[] members;
  private final boolean isLeader;
  private final Map<ClusterMemberId, ConnectionToClusterMember> memberIdentities;
  private final Cluster cluster;
  private final DistributableData distributableData;

  /**
   * Create a new ClusterMember, and join the specified cluster
   *
   * @param cluster The details of the cluster to participate in
   * @throws Exception If joining the cluster fails
   */
  public ClusterMember(Cluster cluster) throws Exception {
    this.cluster = cluster;
    this.jobs = new LinkedBlockingQueue<>();
    this.dataReferenceHashers = new ConcurrentHashMap<>();
    this.distributableData = new DistributableData();
    this.memberServer = ServerBuilder.forPort(cluster.memberPort).addService(this).build().start();

    log.debug("{} - joining lobby", cluster.clusterName);
    try (final var coordinator =
        new ConnectionToCoordinator(cluster.coordinatorHost, cluster.coordinatorPort)) {
      ClusterMembers clusterMembers =
          coordinator.joinCluster(
              cluster.clusterName,
              cluster.memberPort,
              cluster.expectedClusterMembers,
              cluster.coordinatorLobbyTimeout);
      log.debug(
          "{} - cluster started, isLeader={}", cluster.clusterName, clusterMembers.getIsLeader());
      this.members =
          clusterMembers.getClusterMembersList().stream()
              .map(ConnectionToClusterMember::new)
              .toArray(ConnectionToClusterMember[]::new);
      this.isLeader = clusterMembers.getIsLeader();
      log.debug("{} - connected to {} workers", cluster.clusterName, this.members.length);
      this.memberIdentities =
          Arrays.stream(members)
              .collect(
                  toUnmodifiableMap(
                      worker -> ClusterMemberId.fromBytes(worker.identify().getNodeId()),
                      Function.identity()));
    } catch (Throwable t) {
      memberServer.shutdown();
      throw t;
    }
  }

  /**
   * Is this cluster member the leader?
   *
   * @return <code>true</code> if the leader, otherwise <code>false</code>
   */
  public boolean isLeader() {
    return isLeader;
  }

  /**
   * Partition the DataSource into equally sized ranges (one per member), and instruct each member
   * to process its assigned range.
   *
   * @param dataSource The data source to distribute across the cluster
   * @param <I> The type of values in the data source
   * @return A future of collected value iterators for each cluster member
   * @throws IllegalStateException if this member is not the cluster leader
   */
  public <I> List<CompletableFuture<Iterator<Value>>> distributeDataSource(
      DataSource<I> dataSource) {
    if (!isLeader()) {
      throw new IllegalStateException("Only the leader should distributed work to other members");
    }
    final var dataSourceRanges = generateRanges(dataSource.countOfFullSet(), members.length);
    final var memberJobs =
        new ArrayList<CompletableFuture<Iterator<Value>>>(dataSourceRanges.length);
    for (int i = 0; i < dataSourceRanges.length; i++) {
      final var member = members[i];
      final var range = dataSourceRanges[i];
      memberJobs.add(CompletableFuture.supplyAsync(() -> member.process(range)));
    }
    return memberJobs;
  }

  /**
   * Retrieve a range of values from some data referenced on another cluster member. Happens
   * immediately, there is no attempt to do a single-pass of the range.
   *
   * @param memberId The id of the cluster member holding the referenced data
   * @param dataReferenceRange The range of data to retrieve
   * @return An iterator for the Values stored in the given range
   */
  public Iterator<Value> retrieveRangeFromMember(
      ClusterMemberId memberId, DataReferenceRange dataReferenceRange) {
    return getMember(memberId).retrieveRange(dataReferenceRange);
  }

  /**
   * Retrieve all values matching a given hash/modulo. Waits until a classifier/hasher have been
   * specified for the given reference, and then waits until retrievals have been requested for all
   * possible modulo values. Once this happens, a single pass over the data reference values will
   * occur, hashing and distributing values to the waiting retrievers based on their requested
   * modulo.
   *
   * @param memberId The id of the cluster member holding the referenced data
   * @param dataReferenceHashSpec The spec for the data reference and hash/modulo to retrieve
   * @return An iterator for the Values from the referenced data matching the hash/modulo spec
   */
  public Iterator<Value> retrieveByHashFromMember(
      ClusterMemberId memberId, DataReferenceHashSpec dataReferenceHashSpec) {
    return getMember(memberId).retrieveByHash(dataReferenceHashSpec);
  }

  /**
   * Add a DistributableDataReference to the accessible set for other cluster members to retrieve.
   * Expected to be called when persistence-type operations are running
   *
   * @see com.markosindustries.distroboy.core.operations.PersistToDisk
   * @see com.markosindustries.distroboy.core.operations.PersistToHeap
   * @param referenceId The identifier of the reference
   * @param distributableDataReference A distributable reference to some data
   * @param <I> The type of values contained in the referenced data
   */
  public <I> void addDistributableData(
      DataReferenceId referenceId, DistributableDataReference<I> distributableDataReference) {
    distributableData.add(referenceId, distributableDataReference);
  }

  private ConnectionToClusterMember getMember(ClusterMemberId memberId) {
    return memberIdentities.get(memberId);
  }

  @Override
  public void identify(Empty request, StreamObserver<ClusterMemberIdentity> responseObserver) {
    responseObserver.onNext(
        ClusterMemberIdentity.newBuilder().setNodeId(cluster.clusterMemberId.asBytes()).build());
    responseObserver.onCompleted();
  }

  @Override
  public void process(DataSourceRange dataSourceRange, StreamObserver<Value> responseObserver) {
    try {
      log.debug("Starting job {}", describeRange(dataSourceRange));
      Job job = null;
      while (job == null) {
        try {
          job = jobs.poll(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          // Ignore, just try again
        }
      }

      log.debug("Responding to job {}", describeRange(dataSourceRange));
      try (IteratorWithResources<Value> iterator = job.execute(dataSourceRange)) {
        // TODO: backpressure
        while (iterator.hasNext()) {
          final var value = iterator.next();
          responseObserver.onNext(value);
        }
      }
      responseObserver.onCompleted();
      log.debug("Finished job {}", describeRange(dataSourceRange));
    } catch (Exception e) {
      log.error("Failed while executing job", e);
      responseObserver.onError(Status.INTERNAL.asException());
    }
  }

  @Override
  public void distribute(DataReferences request, StreamObserver<Empty> responseObserver) {
    synchronized (distributedReferencesLock) {
      while (distributedReferences.get() != null) {
        try {
          distributedReferencesLock.wait();
        } catch (InterruptedException e) {
          // Ignore, keep trying...
        }
      }
      distributedReferences.set(request);
      distributedReferencesLock.notify();
    }
    responseObserver.onNext(Empty.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void retrieveRange(DataReferenceRange request, StreamObserver<Value> responseObserver) {
    final var memberId = ClusterMemberId.fromBytes(request.getReference().getMemberId());
    if (!cluster.clusterMemberId.equals(memberId)) {
      log.error(
          "Wrong memberId for data range retrieval, this is probably broken -- {} {}",
          memberId,
          cluster.clusterMemberId);
    }

    // TODO: backpressure
    final var distributableDataReference =
        distributableData.retrieve(
            DataReferenceId.fromBytes(request.getReference().getReferenceId()));
    try (final var iterator = distributableDataReference.getSerialisingIterator()) {
      long startInclusive = request.getRange().getStartInclusive();
      long endExclusive = request.getRange().getEndExclusive();
      long currentIndex = 0;
      while (iterator.hasNext()) {
        try {
          final var value = iterator.next();
          if (currentIndex < startInclusive) {
            continue;
          }
          if (currentIndex >= endExclusive) {
            break;
          }
          responseObserver.onNext(value);
        } finally {
          currentIndex++;
        }
      }
      responseObserver.onCompleted();
    } catch (Exception e) {
      log.warn("Failed to close persisted data iterator - likely resource leakage", e);
      responseObserver.onError(Status.INTERNAL.asException());
      throw new RuntimeException(e);
    }
  }

  @Override
  public void retrieveByHash(
      DataReferenceHashSpec request, StreamObserver<Value> responseObserver) {
    final var referenceId = DataReferenceId.fromBytes(request.getReference().getReferenceId());
    final var classifiersForDataReference =
        dataReferenceHashers.computeIfAbsent(referenceId, dr -> new LinkedBlockingQueue<>());
    HashedDataReference<?> hashedDataReference = classifiersForDataReference.peek();
    synchronized (classifiersLock) {
      while (hashedDataReference == null) {
        try {
          classifiersLock.wait();
        } catch (InterruptedException e) {
          // Just try again
        }
        hashedDataReference = classifiersForDataReference.peek();
      }
    }

    synchronized (hashedDataReference.lock) {
      hashedDataReference.retrieversByHash.put(request.getHash(), responseObserver);
      if (hashedDataReference.retrieversByHash.size()
          == hashedDataReference.expectedRetrieveCount) {
        // pop this one off the queue, it's being handled now
        classifiersForDataReference.poll();
        // TODO: backpressure
        final var distributableDataReference = distributableData.retrieve(referenceId);

        try (final var iteratorWithSerialiser =
            distributableDataReference.getIteratorWithSerialiser()) {
          while (iteratorWithSerialiser.hasNext()) {
            final var next = iteratorWithSerialiser.next();
            final var hash = Math.abs(hashedDataReference.hash(next.value) % request.getModulo());
            hashedDataReference.retrieversByHash.get(hash).onNext(next.serialise());
          }
          for (StreamObserver<Value> retriever : hashedDataReference.retrieversByHash.values()) {
            retriever.onCompleted();
          }
        } catch (Exception e) {
          log.warn("Failed to close persisted data iterator - likely resource leakage", e);
          responseObserver.onError(Status.INTERNAL.asException());
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Override
  public void disband(Empty request, StreamObserver<Empty> responseObserver) {
    log.debug("Received disband");
    synchronized (disbandedLock) {
      disbanded.set(true);
      disbandedLock.notify();
    }
    log.debug("Disbanded");
    responseObserver.onNext(Empty.newBuilder().build());
    responseObserver.onCompleted();
  }

  static class HashedDataReference<T> {
    final DataReference dataReference;
    final Function<T, Integer> hasher;
    final int expectedRetrieveCount;
    final Object lock;
    final ConcurrentMap<Integer, StreamObserver<Value>> retrieversByHash;

    public HashedDataReference(
        DataReference dataReference, Function<T, Integer> hasher, int expectedRetrieveCount) {
      this.dataReference = dataReference;
      this.hasher = hasher;
      this.expectedRetrieveCount = expectedRetrieveCount;
      this.lock = new Object();
      this.retrieversByHash = new ConcurrentHashMap<>();
    }

    @SuppressWarnings("unchecked")
    public int hash(Object o) {
      return hasher.apply((T) o);
    }
  }

  /**
   * Add a hash function expected to be used in a retrieval for a part of the job being run
   *
   * @param dataReference The data reference being hashed/retrieved
   * @param hasher The hashing function to be used
   * @param expectedRetrieveCount The number of retrievers who will ask, so that we can wait for
   *     everyone to do a single pass
   * @param <T> The type of values being retrieved/hashed.
   */
  public <T> void pushHasher(
      DataReference dataReference, Function<T, Integer> hasher, int expectedRetrieveCount) {
    synchronized (classifiersLock) {
      final var hashersForDataReference =
          dataReferenceHashers.computeIfAbsent(
              DataReferenceId.fromBytes(dataReference.getReferenceId()),
              dr -> new LinkedBlockingQueue<>());
      hashersForDataReference.add(
          new HashedDataReference<T>(dataReference, hasher, expectedRetrieveCount));
      classifiersLock.notify();
    }
  }

  /**
   * Add a job to be executed by the cluster. Jobs are always executed in the order they are added.
   * This ensures that all cluster members are working on the same job at the same time, since they
   * all have the same code.
   *
   * @param job The next job to be executed
   */
  public void addJob(Job job) {
    jobs.add(job);
  }

  /**
   * As the leader, tell the rest of the cluster about data stored on cluster members
   *
   * @param remoteDataReferences The references to the data being distributed
   */
  public void distributeDataReferences(List<DataReference> remoteDataReferences) {
    for (ConnectionToClusterMember member : members) {
      member.distribute(remoteDataReferences);
    }
  }

  /**
   * Wait for the leader to distribute references to some data stored on cluster members
   *
   * @return The data references distributed by the leader
   * @throws InterruptedException If interrupted while waiting
   */
  public List<DataReference> awaitDistributedDataReferences() throws InterruptedException {
    synchronized (distributedReferencesLock) {
      while (distributedReferences.get() == null) {
        distributedReferencesLock.wait();
      }
      final var referencesList = distributedReferences.getAndSet(null).getReferencesList();
      distributedReferencesLock.notify();
      return referencesList;
    }
  }

  @Override
  public void close() throws Exception {
    log.debug("Closing");
    if (isLeader) {
      log.debug("Closing - sending disbands");
      for (ConnectionToClusterMember member : members) {
        log.debug("Closing - sending disband to {}", member);
        member.disband();
      }
    }
    log.debug("Closing - awaiting disband");
    synchronized (disbandedLock) {
      while (!disbanded.get()) {
        disbandedLock.wait();
      }
    }
    log.debug("Closing - disbanded");
    try {
      memberServer.shutdown();
      memberServer.awaitTermination();
      for (ConnectionToClusterMember member : members) {
        log.debug("Closing - shutting down connection to {}", member);
        member.close();
      }
    } catch (Throwable t) {
      log.error(
          "Couldn't shut down cluster member server/connections - potential resource leakage", t);
      throw t;
    }
  }
}
