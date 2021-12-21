package com.markosindustries.distroboy.kafka;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Provides an {@link Iterator} interface to the data in Kafka for a given set of {@link
 * TopicPartition}s. Reads from the starting offsets (inclusive) to the ending offsets (exclusive)
 * specified.
 *
 * @param <K> The type of keys the {@link org.apache.kafka.clients.consumer.KafkaConsumer} will
 *     deserialise
 * @param <V> The type of values the {@link org.apache.kafka.clients.consumer.KafkaConsumer} will
 *     deserialise
 */
public class KafkaTopicPartitionsIterator<K, V> implements Iterator<ConsumerRecord<K, V>> {
  private final Consumer<K, V> kafkaConsumer;
  private final Set<TopicPartition> partitions;
  private final Map<TopicPartition, Long> startOffsets;
  private final Map<TopicPartition, Long> endOffsets;
  private final LinkedList<ConsumerRecord<K, V>> records = new LinkedList<>();

  /**
   * @param kafkaConsumer A {@link org.apache.kafka.clients.consumer.KafkaConsumer} to communicate
   *     with Kafka via
   * @param topicPartitions The set of {@link TopicPartition}s to iterate records from
   * @param startOffsetsInclusiveSpec The starting offset spec (inclusive)
   * @param endOffsetsExclusiveSpec The end offset spec (exclusive)
   */
  public KafkaTopicPartitionsIterator(
      Consumer<K, V> kafkaConsumer,
      Collection<TopicPartition> topicPartitions,
      KafkaOffsetSpec startOffsetsInclusiveSpec,
      KafkaOffsetSpec endOffsetsExclusiveSpec) {
    this.kafkaConsumer = kafkaConsumer;
    this.partitions = new HashSet<>(topicPartitions);
    this.startOffsets = startOffsetsInclusiveSpec.getOffsets(kafkaConsumer, partitions);
    this.endOffsets = endOffsetsExclusiveSpec.getOffsets(kafkaConsumer, partitions);

    var partitionIntersection = new HashSet<>(startOffsets.keySet());
    partitionIntersection.retainAll(endOffsets.keySet());

    // Remove partitions we don't have a start AND end offset for
    partitions.stream().filter(p -> !partitionIntersection.contains(p)).forEach(partitions::remove);

    // Remove partitions we'll never get records for
    partitionIntersection.stream()
        .filter(p -> !(startOffsets.get(p) < endOffsets.get(p)))
        .forEach(partitions::remove);

    kafkaConsumer.assign(partitions);
    for (final var topicPartition : partitions) {
      kafkaConsumer.seek(topicPartition, startOffsets.get(topicPartition));
    }
    ensureQueueDoesntRunEmpty();
  }

  private void ensureQueueDoesntRunEmpty() {
    while (!partitions.isEmpty() && records.isEmpty()) {
      if (partitions.size() != kafkaConsumer.assignment().size()) {
        kafkaConsumer.assign(partitions);
      }
      final var batch = kafkaConsumer.poll(Duration.ofSeconds(1));
      for (TopicPartition partition : partitions) {
        final var endOffset = endOffsets.get(partition);
        for (ConsumerRecord<K, V> record : batch.records(partition)) {
          if (record.offset() < endOffset) {
            records.add(record);
          }
        }
      }

      partitions.removeIf(
          partition -> kafkaConsumer.position(partition) >= endOffsets.get(partition));
    }
  }

  @Override
  public boolean hasNext() {
    ensureQueueDoesntRunEmpty();
    return !records.isEmpty();
  }

  @Override
  public ConsumerRecord<K, V> next() {
    ensureQueueDoesntRunEmpty();
    return records.pop();
  }
}
