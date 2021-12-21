package com.markosindustries.distroboy.kafka;

import java.util.Collection;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

/**
 * Load the earliest offsets from Kafka for each partition. <b>WARNING:</b> for topics being
 * actively written to, this may generate an inconsistent view from each node in the distroboy
 * cluster. It is often better to use a timestamp in the past to ensure all nodes see the same time
 * window of data.
 */
public class EarliestKafkaOffsetSpec implements KafkaOffsetSpec {
  @Override
  public <K, V> Map<TopicPartition, Long> getOffsets(
      Consumer<K, V> kafkaConsumer, Collection<TopicPartition> topicPartitions) {
    return kafkaConsumer.beginningOffsets(topicPartitions);
  }
}
