package com.markosindustries.distroboy.kafka;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * Provides an {@link Iterator} interface to the data in Kafka for a given set of {@link
 * TopicPartition}s. Reads from the starting offsets (inclusive) to the ending offsets (exclusive)
 * specified.
 *
 * @param <K> The type of keys the {@link KafkaConsumer} will deserialise
 * @param <V> The type of values the {@link KafkaConsumer} will deserialise
 */
public class KafkaTopicPartitionsIterator<K, V>
    implements IteratorWithResources<ConsumerRecord<K, V>> {
  private final Consumer<K, V> kafkaConsumer;
  private final Set<TopicPartition> partitions;
  private final Map<TopicPartition, Long> startOffsets;
  private final Map<TopicPartition, Long> endOffsets;
  private final LinkedList<ConsumerRecord<K, V>> records = new LinkedList<>();

  /**
   * @param kafkaConfiguration A {@link Map} of <a
   *     href="http://kafka.apache.org/documentation.html#consumerconfigs" >Configuration</a> needed
   *     to instantiate a {@link KafkaConsumer} to communicate with Kafka via
   * @param topicPartitions The set of {@link TopicPartition}s to iterate records from
   * @param startOffsetsInclusiveSpec The starting offset spec (inclusive)
   * @param endOffsetsExclusiveSpec The end offset spec (exclusive)
   */
  public KafkaTopicPartitionsIterator(
      final Map<String, Object> kafkaConfiguration,
      final Collection<TopicPartition> topicPartitions,
      final KafkaOffsetSpec startOffsetsInclusiveSpec,
      final KafkaOffsetSpec endOffsetsExclusiveSpec) {
    this.kafkaConsumer = new KafkaConsumer<>(kafkaConfiguration);

    this.startOffsets = startOffsetsInclusiveSpec.getOffsets(kafkaConsumer, topicPartitions);
    this.endOffsets = endOffsetsExclusiveSpec.getOffsets(kafkaConsumer, topicPartitions);

    this.partitions = new HashSet<>(startOffsets.keySet());
    // Remove partitions we don't have a start AND end offset for
    this.partitions.retainAll(endOffsets.keySet());
    // Remove partitions we'll never get records for
    this.partitions.removeIf(p -> startOffsets.get(p) >= endOffsets.get(p));

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

  @Override
  public void close() throws Exception {
    this.kafkaConsumer.close();
  }
}
