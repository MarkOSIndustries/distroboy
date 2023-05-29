package com.markosindustries.distroboy.kafka;

import static java.util.stream.Collectors.toMap;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
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
  public TimestampKafkaOffsetSpec(final Instant instant) {
    this(instant.toEpochMilli());
  }

  /**
   * Return offsets for each partition by looking up the given Kafka timestamp
   *
   * @param timestampMs The UTC timestamp to retrieve offsets for from Kafka (in milliseconds)
   */
  public TimestampKafkaOffsetSpec(final long timestampMs) {
    this.timestampMs = timestampMs;
  }

  @Override
  public <K, V> Map<TopicPartition, Long> getOffsets(
      final Consumer<K, V> kafkaConsumer, final Collection<TopicPartition> topicPartitions) {
    return kafkaConsumer
        .offsetsForTimes(
            topicPartitions.stream().collect(toMap(Function.identity(), _ignored -> timestampMs)))
        .entrySet()
        .stream()
        .filter(entry -> Objects.nonNull(entry.getValue()))
        .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().offset()));
  }
}
