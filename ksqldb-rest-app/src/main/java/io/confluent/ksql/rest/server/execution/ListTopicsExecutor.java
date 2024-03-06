/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.execution;

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.ListTopics;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.KafkaTopicInfo;
import io.confluent.ksql.rest.entity.KafkaTopicInfoExtended;
import io.confluent.ksql.rest.entity.KafkaTopicsList;
import io.confluent.ksql.rest.entity.KafkaTopicsListExtended;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.services.KafkaConsumerGroupClient;
import io.confluent.ksql.services.KafkaConsumerGroupClient.ConsumerSummary;
import io.confluent.ksql.services.KafkaConsumerGroupClientImpl;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.ReservedInternalTopics;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;

public final class ListTopicsExecutor {

  private ListTopicsExecutor() {

  }

  public static StatementExecutorResponse execute(
      final ConfiguredStatement<ListTopics> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final KafkaTopicClient client = serviceContext.getTopicClient();
    final String defaultStream = serviceContext.getKsqlConfig().getKsqlDefaultStream();
    final Map<String, TopicDescription> topicDescriptions =
        listTopics(client, statement, defaultStream);

    if (statement.getStatement().getShowExtended()) {

      final List<KafkaTopicInfoExtended> topicInfoExtendedList = topicDescriptions.values()
          .stream().map(desc ->
              topicDescriptionToTopicInfoExtended(desc))
          .collect(Collectors.toList());

      return StatementExecutorResponse.handled(Optional.of(
          new KafkaTopicsListExtended(statement.getMaskedStatementText(), topicInfoExtendedList)));
    } else {
      final List<KafkaTopicInfo> topicInfoList = topicDescriptions.values()
          .stream().map(ListTopicsExecutor::topicDescriptionToTopicInfo)
          .collect(Collectors.toList());

      return StatementExecutorResponse.handled(Optional.of(
          new KafkaTopicsList(statement.getMaskedStatementText(), topicInfoList)));
    }
  }

  private static Set<String> decorateTopicsWithStreamName(final Collection<String> topics,
                                                          final String stream) {
    return topics.stream().map(x -> String.format("%s:%s", stream, x)).collect(Collectors.toSet());
  }

  private static Map<String, TopicDescription> listTopics(
      final KafkaTopicClient topicClient,
      final ConfiguredStatement<ListTopics> statement,
      final String defaultStream
  ) {
    final ReservedInternalTopics internalTopics =
        new ReservedInternalTopics(statement.getSessionConfig().getConfig(false));
    final Optional<SourceName> stream = statement.getStatement().getStream();

    final String streamName = stream.isPresent()
        ? stream.get().toString(FormatOptions.noEscape())
        : defaultStream;

    final Set<String> topics =
        decorateTopicsWithStreamName(topicClient.listTopicNames(streamName),
            streamName);

    // TreeMap is used to keep elements sorted
    return statement.getStatement().getShowAll()
        ? new TreeMap<>(topicClient.describeTopics(topics))
        : new TreeMap<>(topicClient.describeTopics(internalTopics.removeHiddenTopics(topics)));
  }

  private static KafkaTopicInfo topicDescriptionToTopicInfo(final TopicDescription description) {
    return new KafkaTopicInfo(
        description.name(),
        description.partitions()
            .stream().map(partition -> partition.replicas().size()).collect(Collectors.toList()));
  }

  private static KafkaTopicInfoExtended topicDescriptionToTopicInfoExtended(
      final TopicDescription topicDescription
  ) {

    return new KafkaTopicInfoExtended(
        topicDescription.name(),
        topicDescription.partitions()
            .stream().map(partition -> partition.replicas().size()).collect(Collectors.toList()),
        -1,
        -1);
  }

  /**
   * @return all topics with their associated consumerCount and consumerGroupCount
   */
  private static Map<String, List<Integer>> getTopicConsumerAndGroupCounts(
      final KafkaConsumerGroupClient consumerGroupClient
  ) {

    final List<String> consumerGroups = new ArrayList<>();

    final Map<String, AtomicInteger> topicConsumerCount = new HashMap<>();
    final Map<String, Set<String>> topicConsumerGroupCount = new HashMap<>();

    for (final String group : consumerGroups) {
      final Collection<ConsumerSummary> consumerSummaryList =
          consumerGroupClient.describeConsumerGroup(group).consumers();

      for (final KafkaConsumerGroupClientImpl.ConsumerSummary summary : consumerSummaryList) {

        for (final TopicPartition topicPartition : summary.partitions()) {
          topicConsumerCount
              .computeIfAbsent(topicPartition.topic(), k -> new AtomicInteger())
              .incrementAndGet();
          topicConsumerGroupCount
              .computeIfAbsent(topicPartition.topic(), k -> new HashSet<>()).add(group);
        }
      }
    }
    final HashMap<String, List<Integer>> results = new HashMap<>();
    topicConsumerCount.forEach(
        (k, v) -> {
          results.computeIfAbsent(k, v1 -> new ArrayList<>()).add(v.intValue());
          results.get(k).add(topicConsumerGroupCount.get(k).size());
        }
    );

    return results;
  }
}
