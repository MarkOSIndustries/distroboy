package com.markosindustries.distroboy.kafka;

import java.util.Collection;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

/** Specifies how to load offsets for a given set of Kafka partitions. */
public interface KafkaOffsetSpec {
  /**
   * Load the offsets for a given set of topicPartitions
   *
   * @param kafkaConsumer A {@link org.apache.kafka.clients.consumer.KafkaConsumer} to communicate
   *     with Kafka via
   * @param topicPartitions The set of {@link TopicPartition}s to load offsets for
   * @param <K> The type of keys the {@link org.apache.kafka.clients.consumer.KafkaConsumer} will
   *     deserialise
   * @param <V> The type of values the {@link org.apache.kafka.clients.consumer.KafkaConsumer} will
   *     deserialise
   * @return A map from each TopicPartition specified to it's offset according to this Spec.
   */
  <K, V> Map<TopicPartition, Long> getOffsets(
      Consumer<K, V> kafkaConsumer, Collection<TopicPartition> topicPartitions);
}
