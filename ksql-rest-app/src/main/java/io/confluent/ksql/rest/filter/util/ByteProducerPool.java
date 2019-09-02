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

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.rest.impersonation.Errors;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ByteProducerPool {
  private final Properties properties;
  private Map<UserGroupInformation, KafkaProducer<byte[], byte[]>> producers =
          new ConcurrentHashMap<>();

  @VisibleForTesting
  private Function<Properties, KafkaProducer<byte[], byte[]>> producerFactory;

  public ByteProducerPool(final Properties properties) {
    this.properties = properties;
    this.producerFactory = KafkaProducer::new;
  }

  public Future<RecordMetadata> send(final ProducerRecord<byte[], byte[]> record) {
    try {
      final UserGroupInformation user = UserGroupInformation.getCurrentUser();

      final KafkaProducer<byte[], byte[]> producer
              = producers.computeIfAbsent(user, info -> producerFactory.apply(properties));

      return producer.send(record);
    } catch (Exception e) {
      throw Errors.serverLoginException(e);
    }
  }

  public void close() {
    for (KafkaProducer producer : producers.values()) {
      producer.close();
    }
  }
}
