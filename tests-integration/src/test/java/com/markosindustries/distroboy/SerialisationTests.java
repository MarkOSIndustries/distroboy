package com.markosindustries.distroboy;

import com.markosindustries.distroboy.core.clustering.serialisation.Serialisers;
import com.markosindustries.distroboy.schemas.SortRange;
import com.markosindustries.distroboy.schemas.Value;
import java.io.Serializable;
import java.math.BigInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SerialisationTests {

  public record SomeJavaRecord(int x, String y, BigInteger z) implements Serializable {}

  @Test
  public void canUseInbuiltJavaSerialisation() throws Exception {
    final var expectedJavaRecord = new SomeJavaRecord(234, "a string, my goodness", BigInteger.TEN);

    DistroBoySingleProcess.run(
        "InProcessDistroBoyTest.canUseInbuiltJavaSerialisation",
        3,
        cluster -> {
          final var theJavaRecordFromTheLeader =
              cluster.waitAndReplicateToAllMembers(
                  () -> {
                    return expectedJavaRecord;
                  },
                  Serialisers.javaRecords(SomeJavaRecord.class));

          // Each node checks it got the same record
          Assertions.assertEquals(expectedJavaRecord, theJavaRecordFromTheLeader);
        });
  }

  @Test
  public void canUseProtobufSerialisation() throws Exception {
    final var expectedProtobufMessage =
        SortRange.newBuilder()
            .setRangeStartExclusive(Value.newBuilder().setStringValue("garg"))
            .setRangeEndInclusive(Value.newBuilder().setStringValue("warg"))
            .build();
    DistroBoySingleProcess.run(
        "InProcessDistroBoyTest.canUseProtobufSerialisation",
        3,
        cluster -> {
          final var theProtobufMessageFromTheLeader =
              cluster.waitAndReplicateToAllMembers(
                  () -> {
                    return expectedProtobufMessage;
                  },
                  Serialisers.protobufValues(SortRange::parseFrom));

          // Each node checks it got the same message
          Assertions.assertEquals(expectedProtobufMessage, theProtobufMessageFromTheLeader);
        });
  }
}
