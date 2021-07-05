/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.ksql.rest.server.filter.util;

import io.confluent.rest.impersonation.Errors;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class ByteConsumerPool {

  private final Map<UserGroupInformation, ThreadLocal<KafkaConsumer<byte[], byte[]>>> consumers =
      new ConcurrentHashMap<>();

  private final Properties properties;
  private final String stream;

  public ByteConsumerPool(final Properties properties, final String stream) {
    this.properties = properties;
    this.stream = stream;
  }

  public ConsumerRecords<byte[], byte[]> poll() {
    try {
      final UserGroupInformation user = UserGroupInformation.getCurrentUser();
      final ThreadLocal<KafkaConsumer<byte[], byte[]>> threadLocal =
          consumers.computeIfAbsent(user, info -> new ThreadLocal<>());

      KafkaConsumer<byte[], byte[]> kafkaConsumer = threadLocal.get();
      if (Objects.isNull(kafkaConsumer)) {
        kafkaConsumer = new KafkaConsumer<>(createPropsWithConsumerGroup(properties, user));
        kafkaConsumer.assign(Collections.singletonList(new TopicPartition(stream, 0)));
        threadLocal.set(kafkaConsumer);
      }

      return kafkaConsumer.poll(Duration.ofMillis(1000));
    } catch (Exception e) {
      throw Errors.serverLoginException(e);
    }
  }

  private Properties createPropsWithConsumerGroup(final Properties consumerProps,
                                                  final UserGroupInformation user) {
    final Properties properties = new Properties();
    properties.putAll(consumerProps);
    final String group =
        String.format("%d_%s", Thread.currentThread().getId(), user.getUserName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);

    return properties;
  }
}
