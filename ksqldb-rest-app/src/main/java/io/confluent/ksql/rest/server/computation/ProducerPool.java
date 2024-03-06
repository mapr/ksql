/**
 * <p>Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at</p>
 *
 *     <p>http://www.apache.org/licenses/LICENSE-2.0</p>
 *
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.</p>
 */

package io.confluent.ksql.rest.server.computation;

import io.confluent.ksql.rest.entity.CommandId;
import io.confluent.rest.impersonation.Errors;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.producer.KafkaProducer;
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
    this.configs = new HashMap<>(configs);
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  public KafkaProducer<CommandId, Command> getProducer() {
    try {
      final UserGroupInformation user = UserGroupInformation.getCurrentUser();

      return producers.computeIfAbsent(user,
          k -> new KafkaProducer<>(configs, keySerializer, valueSerializer));

    } catch (IOException e) {
      throw Errors.serverLoginException(e);
    }
  }

  public void close() {
    for (KafkaProducer<CommandId, Command> producer : producers.values()) {
      producer.close();
    }
  }
}
