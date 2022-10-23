package com.markosindustries.distroboy;

import static java.util.stream.Collectors.groupingBy;

import com.markosindustries.distroboy.core.Hashing;
import com.markosindustries.distroboy.core.clustering.ClusterMemberId;
import com.markosindustries.distroboy.core.clustering.serialisation.Serialisers;
import com.markosindustries.distroboy.core.operations.DistributedOpSequence;
import com.markosindustries.distroboy.core.operations.StaticDataSource;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class InProcessDistroBoyTest {
  @Test
  public void canRunAJob() throws Exception {
    final var expectedValues = List.of(1, 2, 3, 4);
    DistroBoySingleProcess.run(
        "InProcessDistroBoyTest.canRunAJob",
        2,
        cluster -> {
          final var simpleJob =
              DistributedOpSequence.readFrom(new StaticDataSource<>(expectedValues)).count();
          cluster
              .execute(simpleJob)
              .onClusterLeader(
                  count -> {
                    Assertions.assertEquals(expectedValues.size(), count);
                  });
        });
  }

  @Test
  public void canUseForEach() throws Exception {
    final var expectedValues = List.of(1, 2, 3, 4);
    final var actualValues = new ConcurrentSkipListSet<>();

    DistroBoySingleProcess.run(
        "InProcessDistroBoyTest.canUseForEach",
        2,
        cluster -> {
          final var simpleJob =
              DistributedOpSequence.readFrom(new StaticDataSource<>(expectedValues))
                  .forEach(actualValues::add);
          cluster
              .execute(simpleJob)
              .onClusterLeader(
                  ignored -> {
                    Assertions.assertIterableEquals(expectedValues, actualValues);
                  });
        });
  }

  @Test
  public void canUseRedistributeAndGroupBy() throws Exception {
    final var modulo = 2;
    final var expectedValues = List.of(1, 2, 3, 4);
    final var expectedMap = expectedValues.stream().collect(groupingBy(x -> x % modulo));

    DistroBoySingleProcess.run(
        "InProcessDistroBoyTest.canUseRedistributeAndGroupBy",
        2,
        cluster -> {
          final var groupedDataJob =
              DistributedOpSequence.readFrom(new StaticDataSource<>(expectedValues))
                  .redistributeAndGroupBy(
                      cluster,
                      num -> num % modulo,
                      Hashing::integers,
                      modulo,
                      Serialisers.integerValues)
                  .collect(
                      Serialisers.mapEntries(
                          Serialisers.integerValues,
                          Serialisers.listEntries(Serialisers.integerValues)));

          cluster
              .execute(groupedDataJob)
              .onClusterLeader(
                  actualMap -> {
                    Assertions.assertIterableEquals(expectedMap.keySet(), actualMap.keySet());
                    Assertions.assertIterableEquals(
                        expectedMap.values().stream()
                            .map(x -> x.stream().sorted().collect(Collectors.toUnmodifiableList()))
                            .collect(Collectors.toUnmodifiableList()),
                        actualMap.values().stream()
                            .map(x -> x.stream().sorted().collect(Collectors.toUnmodifiableList()))
                            .collect(Collectors.toUnmodifiableList()));
                  });
        });
  }

  @Test
  public void canUseRedistributeEvenlyFromHeap() throws Exception {
    final var expectedValues = List.of(1, 2, 3, 4);

    DistroBoySingleProcess.run(
        "InProcessDistroBoyTest.canUseRedistributeEvenlyFromHeap",
        2,
        cluster -> {
          final var simpleJob =
              DistributedOpSequence.readFrom(new StaticDataSource<>(expectedValues))
                  .persistToHeap(cluster, Serialisers.integerValues);

          final var dataReferences = cluster.persist(simpleJob);

          final var redistributeJob =
              cluster
                  .redistributeEqually(dataReferences, Serialisers.integerValues)
                  .map(x -> cluster.clusterMemberId.asBytes())
                  .collect(Serialisers.byteStringValues);

          cluster
              .execute(redistributeJob)
              .onClusterLeader(
                  memberIds -> {
                    Assertions.assertEquals(expectedValues.size(), memberIds.size());

                    final var uniqueMemberIds =
                        memberIds.stream()
                            .map(ClusterMemberId::fromBytes)
                            .collect(Collectors.toUnmodifiableSet());

                    Assertions.assertEquals(expectedValues.size() / 2, uniqueMemberIds.size());
                  });
        });
  }

  @Test
  public void canUseRedistributeEvenlyFromDisk() throws Exception {
    final var expectedValues = List.of(1, 2, 3, 4);

    DistroBoySingleProcess.run(
        "InProcessDistroBoyTest.canUseRedistributeEvenlyFromDisk",
        2,
        cluster -> {
          final var simpleJob =
              DistributedOpSequence.readFrom(new StaticDataSource<>(expectedValues))
                  .persistToDisk(cluster, Serialisers.integerValues);

          final var dataReferences = cluster.persist(simpleJob);

          final var redistributeJob =
              cluster
                  .redistributeEqually(dataReferences, Serialisers.integerValues)
                  .map(x -> cluster.clusterMemberId.asBytes())
                  .collect(Serialisers.byteStringValues);

          cluster
              .execute(redistributeJob)
              .onClusterLeader(
                  memberIds -> {
                    Assertions.assertEquals(expectedValues.size(), memberIds.size());

                    final var uniqueMemberIds =
                        memberIds.stream()
                            .map(ClusterMemberId::fromBytes)
                            .collect(Collectors.toUnmodifiableSet());

                    Assertions.assertEquals(expectedValues.size() / 2, uniqueMemberIds.size());
                  });
        });
  }

  @Test
  public void canSynchroniseMembers() throws Exception {
    DistroBoySingleProcess.run(
        "InProcessDistroBoyTest.canSynchroniseMembers",
        2,
        cluster -> {
          AtomicReference<Long> timeOnLeaderRef = new AtomicReference<>();
          final var timeOnLeader =
              cluster.waitAndDistributeToAllMembers(
                  () -> {
                    timeOnLeaderRef.set(System.currentTimeMillis());
                    return timeOnLeaderRef.get();
                  },
                  Serialisers.longValues);

          final var simpleJob =
              DistributedOpSequence.readFrom(new StaticDataSource<>(List.of(timeOnLeader)))
                  .collect(Serialisers.longValues);
          cluster
              .execute(simpleJob)
              .onClusterLeader(
                  result -> {
                    Assertions.assertEquals(1, result.size());
                    Assertions.assertEquals(timeOnLeaderRef.get(), result.get(0));
                  });
        });
  }
}
