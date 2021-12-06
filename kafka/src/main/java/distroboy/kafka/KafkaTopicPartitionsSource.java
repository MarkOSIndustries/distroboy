package distroboy.kafka;

import distroboy.core.iterators.IteratorWithResources;
import distroboy.core.operations.DataSource;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class KafkaTopicPartitionsSource implements DataSource<List<TopicPartition>> {
  private final Collection<String> topics;
  private final List<TopicPartition> topicPartitions;

  public KafkaTopicPartitionsSource(Consumer<?, ?> kafkaConsumer, Collection<String> topics) {
    this.topics = topics;
    this.topicPartitions =
        topics.stream()
            .flatMap(
                topic ->
                    kafkaConsumer.partitionsFor(topic).stream()
                        .sorted(Comparator.comparingInt(PartitionInfo::partition)))
            .map(
                partitionInfo ->
                    new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
            .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public long countOfFullSet() {
    return topicPartitions.size();
  }

  @Override
  public IteratorWithResources<List<TopicPartition>> enumerateRangeOfFullSet(
      long startInclusive, long endExclusive) {
    return IteratorWithResources.of(
        topicPartitions.stream()
            .skip(startInclusive)
            .limit(endExclusive - startInclusive)
            .collect(Collectors.toUnmodifiableList()));
  }
}