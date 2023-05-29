package com.markosindustries.distroboy.kafka;

import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

/**
 * Returns offsets from a set of offset specs in the order specified until one has been found for
 * each partition requested, or there are no more offset specs in the list
 */
public class PrioritisedOffsetSpecs implements KafkaOffsetSpec {
  private final List<KafkaOffsetSpec> prioritisedOffsetSpecs;

  /**
   * Returns offsets from a set of offset specs in the order specified until one has been found for
   * each partition requested, or there are no more offset specs in the list
   *
   * @param prioritisedOffsetSpecs The ordered list of offset specs to use
   */
  public PrioritisedOffsetSpecs(final KafkaOffsetSpec... prioritisedOffsetSpecs) {
    this(List.of(prioritisedOffsetSpecs));
  }

  /**
   * Returns offsets from a set of offset specs in the order specified until one has been found for
   * each partition requested, or there are no more offset specs in the list
   *
   * @param prioritisedOffsetSpecs The ordered list of offset specs to use
   */
  public PrioritisedOffsetSpecs(final List<KafkaOffsetSpec> prioritisedOffsetSpecs) {
    this.prioritisedOffsetSpecs = List.copyOf(prioritisedOffsetSpecs);
  }

  @Override
  public <K, V> Map<TopicPartition, Long> getOffsets(
      final Consumer<K, V> kafkaConsumer, final Collection<TopicPartition> topicPartitions) {
    final var result = ImmutableMap.<TopicPartition, Long>builder();
    final var remainingTopicPartitions = new HashSet<>(topicPartitions);
    final var offsetSpecs = prioritisedOffsetSpecs.iterator();
    while (offsetSpecs.hasNext() && !remainingTopicPartitions.isEmpty()) {
      final var offsetSpec = offsetSpecs.next();
      final var offsets = offsetSpec.getOffsets(kafkaConsumer, remainingTopicPartitions);
      for (final var entry : offsets.entrySet()) {
        final var topicPartition = entry.getKey();
        final var offset = entry.getValue();
        result.put(topicPartition, offset);
        remainingTopicPartitions.remove(topicPartition);
      }
    }
    return result.build();
  }
}
