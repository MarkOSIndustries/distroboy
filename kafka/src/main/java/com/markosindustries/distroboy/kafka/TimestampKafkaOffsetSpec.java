package com.markosindustries.distroboy.kafka;

import static java.util.stream.Collectors.toMap;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

/** Return offsets for each partition by looking up the given timestamp. */
public class TimestampKafkaOffsetSpec implements KafkaOffsetSpec {
  private final long timestampMs;

  /**
   * Return offsets for each partition by looking up the given Instant as a Kafka timestamp
   *
   * @param instant The time to retrieve offsets for from Kafka
   */
  public TimestampKafkaOffsetSpec(Instant instant) {
    this(instant.toEpochMilli());
  }

  /**
   * Return offsets for each partition by looking up the given Kafka timestamp
   *
   * @param timestampMs The UTC timestamp to retrieve offsets for from Kafka (in milliseconds)
   */
  public TimestampKafkaOffsetSpec(long timestampMs) {
    this.timestampMs = timestampMs;
  }

  @Override
  public <K, V> Map<TopicPartition, Long> getOffsets(
      Consumer<K, V> kafkaConsumer, Collection<TopicPartition> topicPartitions) {
    return kafkaConsumer
        .offsetsForTimes(
            topicPartitions.stream().collect(toMap(Function.identity(), _ignored -> timestampMs)))
        .entrySet()
        .stream()
        .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().offset()));
  }
}
