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

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ByteProducerPool {

  private Map<UserGroupInformation, KafkaProducer<byte[], byte[]>> producers =
      new ConcurrentHashMap<>();

  private Properties properties;

  public ByteProducerPool(final Properties properties) {
    this.properties = properties;
  }

  public Future<RecordMetadata> send(final ProducerRecord<byte[], byte[]> record) {
    try {
      final UserGroupInformation user = UserGroupInformation.getCurrentUser();

      final KafkaProducer<byte[], byte[]> producer
          = producers.computeIfAbsent(user, k -> new KafkaProducer<>(properties));

      return producer.send(record);
    } catch (IOException e) {
      throw Errors.serverLoginException(e);
    }
  }

  public void close() {
    for (KafkaProducer<byte[], byte[]> producer : producers.values()) {
      producer.close();
    }
  }
}
