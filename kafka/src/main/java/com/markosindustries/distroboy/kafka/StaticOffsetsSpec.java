package com.markosindustries.distroboy.kafka;

import static java.util.stream.Collectors.toMap;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

public class StaticOffsetsSpec implements KafkaOffsetSpec {
  private final Map<TopicPartition, Long> staticOffsets;

  public StaticOffsetsSpec(final Map<TopicPartition, Long> staticOffsets) {
    this.staticOffsets = staticOffsets;
  }

  @Override
  public <K, V> Map<TopicPartition, Long> getOffsets(
      final Consumer<K, V> kafkaConsumer, final Collection<TopicPartition> topicPartitions) {
    return topicPartitions.stream()
        .filter(staticOffsets::containsKey)
        .collect(toMap(Function.identity(), staticOffsets::get));
  }
}
