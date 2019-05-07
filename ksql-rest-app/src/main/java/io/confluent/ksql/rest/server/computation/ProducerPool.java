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

package io.confluent.ksql.rest.server.computation;

import io.confluent.rest.impersonation.Errors;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;

public class ProducerPool {

  private Map<UserGroupInformation, KafkaProducer<CommandId, Command>> producers =
          new ConcurrentHashMap<>();

  private Map<String, Object> configs;
  private Serializer<CommandId> keySerializer;
  private Serializer<Command> valueSerializer;

  public ProducerPool(final Map<String, Object> configs,
                      final Serializer<CommandId> keySerializer,
                      final Serializer<Command> valueSerializer) {
    this.configs = configs;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  public Future<RecordMetadata> send(final ProducerRecord<CommandId, Command> record) {
    try {
      final UserGroupInformation user = UserGroupInformation.getCurrentUser();

      final KafkaProducer<CommandId, Command> producer
              = producers.computeIfAbsent(user,
                k -> new KafkaProducer<>(configs, keySerializer, valueSerializer));

      return producer.send(record);
    } catch (IOException e) {
      throw Errors.serverLoginException(e);
    }
  }

  public void close() {
    for (KafkaProducer producer : producers.values()) {
      producer.close();
    }
  }
}
