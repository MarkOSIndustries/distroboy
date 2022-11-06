package com.markosindustries.distroboy.kafka;

import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.operations.FlatMapOp;
import java.util.List;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * FlatMap a given set of TopicPartitions by loading all records from the starting offsets
 * (inclusive) to the ending offsets (exclusive) specified.
 *
 * @param <K> The type of keys the {@link org.apache.kafka.clients.consumer.KafkaConsumer} will
 *     deserialise
 * @param <V> The type of values the {@link org.apache.kafka.clients.consumer.KafkaConsumer} will
 *     deserialise
 */
public class ReadKafkaTopicPartitionRange<K, V>
    implements FlatMapOp<List<TopicPartition>, ConsumerRecord<K, V>> {
  private final Consumer<K, V> kafkaConsumer;
  private final KafkaOffsetSpec startOffsetInclusiveSpec;
  private final KafkaOffsetSpec endOffsetExclusiveSpec;

  /**
   * @param kafkaConsumerSupplier A Supplier of {@link
   *     org.apache.kafka.clients.consumer.KafkaConsumer} to communicate with Kafka via
   * @param startOffsetInclusiveSpec The starting offset spec (inclusive)
   * @param endOffsetExclusiveSpec The end offset spec (exclusive)
   */
  public ReadKafkaTopicPartitionRange(
      Supplier<Consumer<K, V>> kafkaConsumerSupplier,
      KafkaOffsetSpec startOffsetInclusiveSpec,
      KafkaOffsetSpec endOffsetExclusiveSpec) {
    this.kafkaConsumer = kafkaConsumerSupplier.get();
    this.startOffsetInclusiveSpec = startOffsetInclusiveSpec;
    this.endOffsetExclusiveSpec = endOffsetExclusiveSpec;
  }

  @Override
  public IteratorWithResources<ConsumerRecord<K, V>> flatMap(List<TopicPartition> input) {
    return IteratorWithResources.from(
        new KafkaTopicPartitionsIterator<K, V>(
            kafkaConsumer, input, startOffsetInclusiveSpec, endOffsetExclusiveSpec));
  }
}
