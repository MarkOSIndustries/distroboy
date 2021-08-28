package distroboy.kafka;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

public class KafkaTopicPartitionsIterator<K, V> implements Iterator<ConsumerRecord<K, V>> {
  private final Consumer<K, V> kafkaConsumer;
  private final Set<TopicPartition> partitions;
  private final Map<TopicPartition, Long> startOffsets;
  private final Map<TopicPartition, Long> endOffsets;
  private final LinkedList<ConsumerRecord<K, V>> records = new LinkedList<>();

  public KafkaTopicPartitionsIterator(
      Consumer<K, V> kafkaConsumer,
      Collection<TopicPartition> topicPartitions,
      KafkaOffsetSpec startOffsetsInclusiveSpec,
      KafkaOffsetSpec endOffsetsExclusiveSpec) {
    this.kafkaConsumer = kafkaConsumer;
    this.partitions = new HashSet<>(topicPartitions);
    this.startOffsets = startOffsetsInclusiveSpec.getOffsets(kafkaConsumer, topicPartitions);
    this.endOffsets = endOffsetsExclusiveSpec.getOffsets(kafkaConsumer, topicPartitions);

    var partitionIntersection = new HashSet<>(startOffsets.keySet());
    partitionIntersection.retainAll(endOffsets.keySet());

    // Remove partitions we don't have a start AND end offset for
    topicPartitions.removeAll(
        topicPartitions.stream()
            .filter(p -> !partitionIntersection.contains(p))
            .collect(Collectors.toUnmodifiableList()));

    // Remove partitions we'll never get records for
    topicPartitions.removeAll(
        partitionIntersection.stream()
            .filter(p -> !(startOffsets.get(p) < endOffsets.get(p)))
            .collect(Collectors.toUnmodifiableList()));

    kafkaConsumer.assign(topicPartitions);
    for (TopicPartition topicPartition : topicPartitions) {
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
