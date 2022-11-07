package com.markosindustries.distroboy.kafka;

import com.google.common.collect.ImmutableMap;
import com.markosindustries.distroboy.core.iterators.IteratorWithResources;
import com.markosindustries.distroboy.core.operations.FlatMapOp;
import java.util.List;
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
  private final ImmutableMap<String, Object> kafkaConfiguration;
  private final KafkaOffsetSpec startOffsetInclusiveSpec;
  private final KafkaOffsetSpec endOffsetExclusiveSpec;

  /**
   * @param kafkaConfiguration An ImmutableMap of <a
   *     href="http://kafka.apache.org/documentation.html#consumerconfigs">Configuration</a> needed
   *     * to instantiate a KafkaConsumer {@link org.apache.kafka.clients.consumer.KafkaConsumer} to
   *     * communicate with Kafka via
   * @param startOffsetInclusiveSpec The starting offset spec (inclusive)
   * @param endOffsetExclusiveSpec The end offset spec (exclusive)
   */
  public ReadKafkaTopicPartitionRange(
      ImmutableMap<String, Object> kafkaConfiguration,
      KafkaOffsetSpec startOffsetInclusiveSpec,
      KafkaOffsetSpec endOffsetExclusiveSpec) {
    this.kafkaConfiguration = kafkaConfiguration;
    this.startOffsetInclusiveSpec = startOffsetInclusiveSpec;
    this.endOffsetExclusiveSpec = endOffsetExclusiveSpec;
  }

  @Override
  public IteratorWithResources<ConsumerRecord<K, V>> flatMap(List<TopicPartition> input) {
    return new KafkaTopicPartitionsIterator<K, V>(
        kafkaConfiguration, input, startOffsetInclusiveSpec, endOffsetExclusiveSpec);
  }
}
