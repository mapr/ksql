/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.filter.util;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.rest.impersonation.Errors;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class ByteConsumerPool {

  private final Map<UserGroupInformation, ThreadLocal<KafkaConsumer<byte[], byte[]>>> consumers =
          new ConcurrentHashMap<>();

  private final Properties properties;
  private final String topic;

  @VisibleForTesting
  private Function<Properties, KafkaConsumer<byte[], byte[]>> consumerFactory;

  public ByteConsumerPool(Properties properties, String topic) {
    this.properties = properties;
    this.topic = topic;
    this.consumerFactory = KafkaConsumer::new;
  }

  public ConsumerRecords<byte[], byte[]> poll() {
    try {
      final UserGroupInformation user = UserGroupInformation.getCurrentUser();
      final ThreadLocal<KafkaConsumer<byte[], byte[]>> threadLocal =
              consumers.computeIfAbsent(user, info -> createConsumer(user));

      return threadLocal.get().poll(Duration.ofMillis(1000));
    } catch (Exception e) {
      throw Errors.serverLoginException(e);
    }
  }

  private ThreadLocal<KafkaConsumer<byte[], byte[]>> createConsumer(UserGroupInformation user) {
    return ThreadLocal.withInitial(() -> {
      final Properties props = createPropsWithConsumerGroup(properties, user);
      final KafkaConsumer<byte[], byte[]> kafkaConsumer = consumerFactory.apply(props);
      kafkaConsumer.assign(Collections.singletonList(new TopicPartition(topic, 0)));
      return kafkaConsumer;
    });
  }

  private Properties createPropsWithConsumerGroup(Properties consumerProps,
                                                  UserGroupInformation user) {
    final Properties properties = new Properties();
    properties.putAll(consumerProps);
    final String group = String.format("%d_%s", Thread.currentThread().getId(), user.getUserName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);

    return properties;
  }
}
