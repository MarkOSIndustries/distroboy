package com.markosindustries.distroboy.core.clustering;

import static com.markosindustries.distroboy.core.DataSourceRanges.generateRanges;
import static java.util.stream.Collectors.toUnmodifiableMap;

import com.markosindustries.distroboy.core.Cluster;
import com.markosindustries.distroboy.core.DistributedSampleSort;
import com.markosindustries.distroboy.core.clustering.serialisation.Serialiser;
import com.markosindustries.distroboy.core.clustering.serialisation.Serialisers;
import com.markosindustries.distroboy.core.operations.DataSource;
import com.markosindustries.distroboy.schemas.ClusterMembers;
import com.markosindustries.distroboy.schemas.DataReference;
import com.markosindustries.distroboy.schemas.DataReferenceHashSpec;
import com.markosindustries.distroboy.schemas.DataReferenceRange;
import com.markosindustries.distroboy.schemas.DataReferenceSortRange;
import com.markosindustries.distroboy.schemas.Value;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a ClusterMember's capabilities and responsibilities Responsible for cluster
 * startup, answering queries from other members, and delegating work and queries to other cluster
 * members.
 */
public class ClusterMember implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(ClusterMember.class);

  private final Cluster cluster;
  private final Server memberServer;
  private final ConnectionToClusterMember[] members;
  private final Map<ClusterMemberId, ConnectionToClusterMember> memberIdentities;
  private final ClusterMemberState clusterMemberState;

  /**
   * Create a new ClusterMember, and join the specified cluster
   *
   * @param cluster The details of the cluster to participate in
   * @throws Exception If joining the cluster fails
   */
  public ClusterMember(final Cluster cluster) throws Exception {
    this.cluster = cluster;
    this.clusterMemberState = new ClusterMemberState();
    this.memberServer =
        ServerBuilder.forPort(cluster.memberPort)
            .addService(new ClusterMemberListener(cluster, clusterMemberState))
            .intercept(new ServerCallAddressInterceptor())
            .build()
            .start();

    log.debug("{} - joining lobby", cluster.clusterName);
    try (final var coordinator =
        new ConnectionToCoordinator(
            cluster.coordinatorHost, cluster.coordinatorPort, cluster.coordinatorLobbyTimeout)) {
      ClusterMembers clusterMembers =
          coordinator.joinCluster(
              cluster.clusterName, cluster.memberPort, cluster.expectedClusterMembers);
      this.clusterMemberState.isLeader = clusterMembers.getIsLeader();
      log.debug(
          "{} - cluster started, isLeader={}, memberId={}",
          cluster.clusterName,
          this.clusterMemberState.isLeader,
          cluster.clusterMemberId);
      this.members =
          clusterMembers.getClusterMembersList().stream()
              .map(ConnectionToClusterMember::new)
              .toArray(ConnectionToClusterMember[]::new);

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
    return clusterMemberState.isLeader;
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

  public Iterator<Value> retrieveSortSamplesFromMember(
      ClusterMemberId memberId, DataReference dataReference) {
    return getMember(memberId).retrieveSortSamples(dataReference);
  }

  public Iterator<Value> retrieveSortRangeFromMember(
      ClusterMemberId memberId, DataReferenceSortRange dataReferenceSortRange) {
    return getMember(memberId).retrieveSortRange(dataReferenceSortRange);
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
    clusterMemberState.distributableData.add(referenceId, distributableDataReference);
  }

  private ConnectionToClusterMember getMember(ClusterMemberId memberId) {
    return memberIdentities.get(memberId);
  }

  private final AtomicInteger synchronisationIndex = new AtomicInteger(0);

  public <T> T synchroniseValueFromLeader(
      Supplier<T> valueSupplier, Serialiser<T> valueSerialiser, int expectedSynchroniseCount) {
    final int index = synchronisationIndex.getAndIncrement();
    return synchroniseValueFromLeader(
        index, valueSupplier, valueSerialiser, expectedSynchroniseCount);
  }

  private void disband() {
    synchroniseValueFromLeader(-1, () -> null, Serialisers.voidValues, members.length);
  }

  private <T> T synchroniseValueFromLeader(
      int index,
      Supplier<T> valueSupplier,
      Serialiser<T> valueSerialiser,
      int expectedSynchroniseCount) {

    if (isLeader()) {
      synchronized (clusterMemberState.synchronisationPointsLock) {
        clusterMemberState.synchronisationPoints.add(
            new ClusterMemberState.SynchronisationPoint(
                index, valueSupplier, valueSerialiser, expectedSynchroniseCount));
        // Notify as many threads as could possibly be waiting
        for (int i = 0; i < members.length; i++) {
          clusterMemberState.synchronisationPointsLock.notify();
        }
      }
    }

    for (final ConnectionToClusterMember member : members) {
      final Optional<Value> synchronisedValue = member.synchronise(index);
      if (synchronisedValue.isPresent()) {
        try {
          return valueSerialiser.deserialise(synchronisedValue.get());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    // The leader should have responded, something is very broken
    throw new RuntimeException("No member responded with a synchronised value");
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
    clusterMemberState.dataReferenceHashers.supplyValue(
        DataReferenceId.fromBytes(dataReference.getReferenceId()),
        new ClusterMemberState.HashedDataReference<T>(
            dataReference, hasher, expectedRetrieveCount));
  }

  public void takeSortSamples(DataReference dataReference, int sampleCount) {
    final var samples =
        DistributedSampleSort.takeEquidistantSamples(
            dataReference.getCount(),
            clusterMemberState
                .distributableData
                .retrieve(DataReferenceId.fromBytes(dataReference.getReferenceId()))
                .getSerialisingIterator(),
            sampleCount);

    clusterMemberState.dataReferenceSortSamples.supplyValue(
        DataReferenceId.fromBytes(dataReference.getReferenceId()),
        new ClusterMemberState.DataReferenceSortSamples(dataReference, samples));
  }

  public <T> void pushComparator(
      final DataReference dataReference,
      final Comparator<T> comparator,
      final Serialiser<T> serialiser,
      final int expectedRetrieveCount) {
    clusterMemberState.dataReferenceComparators.supplyValue(
        DataReferenceId.fromBytes(dataReference.getReferenceId()),
        new ClusterMemberState.SortedDataReference<T>(
            dataReference, comparator, serialiser, expectedRetrieveCount));
  }

  /**
   * Add a job to be executed by the cluster. Jobs are always executed in the order they are added.
   * This ensures that all cluster members are working on the same jobs in the same order, since
   * they all have the same code.
   *
   * @param job The next job to be executed
   */
  public void addJob(Job job) {
    clusterMemberState.jobs.add(job);
  }

  /**
   * As the leader, tell the rest of the cluster about data stored on cluster members
   *
   * @param remoteDataReferences The references to the data being distributed
   */
  public void distributeDataReferences(List<DataReference> remoteDataReferences) {
    for (final var member : memberIdentities.entrySet()) {
      log.debug("Distributing data references to {}", member.getKey());
      member.getValue().distribute(remoteDataReferences);
    }
  }

  /**
   * Wait for the leader to distribute references to some data stored on cluster members
   *
   * @return The data references distributed by the leader
   * @throws InterruptedException If interrupted while waiting
   */
  public List<DataReference> awaitDistributedDataReferences() throws InterruptedException {
    synchronized (clusterMemberState.distributedReferencesLock) {
      while (clusterMemberState.distributedReferences.get() == null) {
        clusterMemberState.distributedReferencesLock.wait();
      }
      final var referencesList =
          clusterMemberState.distributedReferences.getAndSet(null).getReferencesList();
      clusterMemberState.distributedReferencesLock.notify();
      return referencesList;
    }
  }

  @Override
  public void close() throws Exception {
    log.debug("Closing {} isLeader={}", cluster.clusterMemberId, isLeader());

    try {
      log.debug("Closing - disbanding {}", members.length);
      disband();
      log.debug("Closing - disbanding");
    } catch (Exception ex) {
      log.debug("Closing - disband threw, forcing...", ex);
      for (final ConnectionToClusterMember member : members) {
        try {
          member.forceDisband();
        } catch (Exception unused) {
          // Nothing more to do
        }
      }
    }

    try {
      memberServer.shutdown();
      for (ConnectionToClusterMember member : members) {
        log.debug("Closing - shutting down {}", member);
        member.close();
        log.debug("Closing - successfully shut down {}", member);
      }
      while (!memberServer.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
        log.debug("Awaiting shutdown of member server");
      }
    } catch (Throwable t) {
      log.error(
          "Couldn't shut down cluster member server/connections - potential resource leakage", t);
      throw t;
    }
  }
}
