package com.markosindustries.distroboy;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import com.markosindustries.distroboy.core.Hashing;
import com.markosindustries.distroboy.core.clustering.ClusterMemberId;
import com.markosindustries.distroboy.core.clustering.serialisation.Serialisers;
import com.markosindustries.distroboy.core.operations.DistributedOpSequence;
import com.markosindustries.distroboy.core.operations.StaticDataSource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class InProcessDistroBoyTest {
  @Test
  public void canRunAJob() throws Exception {
    final var expectedValues = List.of(1, 2, 3, 4, 5);
    DistroBoySingleProcess.run(
        "InProcessDistroBoyTest.canRunAJob",
        3,
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
    final var expectedValues = List.of(1, 2, 3, 4, 5);
    final var actualValues = Collections.synchronizedList(new ArrayList<Integer>());

    DistroBoySingleProcess.run(
        "InProcessDistroBoyTest.canUseForEach",
        3,
        cluster -> {
          final var simpleJob =
              DistributedOpSequence.readFrom(new StaticDataSource<>(expectedValues))
                  .forEach(actualValues::add);
          cluster
              .execute(simpleJob)
              .onClusterLeader(
                  ignored -> {
                    Assertions.assertIterableEquals(
                        expectedValues,
                        actualValues.stream().sorted().collect(Collectors.toList()));
                  });
        });
  }

  @Test
  public void canUseRedistributeAndGroupBy() throws Exception {
    final var modulo = 6;
    final var expectedValues = IntStream.range(1, 21).boxed().collect(toList());
    final var expectedMap = expectedValues.stream().collect(groupingBy(x -> x % modulo));

    DistroBoySingleProcess.run(
        "InProcessDistroBoyTest.canUseRedistributeAndGroupBy",
        3,
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
    final var expectedValues = List.of(1, 2, 3, 4, 5);

    DistroBoySingleProcess.run(
        "InProcessDistroBoyTest.canUseRedistributeEvenlyFromHeap",
        3,
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

                    Assertions.assertEquals(3, uniqueMemberIds.size());
                  });
        });
  }

  @Test
  public void canUseRedistributeEvenlyFromDisk() throws Exception {
    final var expectedValues = List.of(1, 2, 3, 4, 5);

    DistroBoySingleProcess.run(
        "InProcessDistroBoyTest.canUseRedistributeEvenlyFromDisk",
        3,
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

                    Assertions.assertEquals(3, uniqueMemberIds.size());
                  });
        });
  }

  @Test
  public void canSynchroniseMembers() throws Exception {
    DistroBoySingleProcess.run(
        "InProcessDistroBoyTest.canSynchroniseMembers",
        3,
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
