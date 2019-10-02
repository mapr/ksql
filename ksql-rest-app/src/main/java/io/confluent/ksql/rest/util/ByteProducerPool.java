/**
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.ksql.rest.util;

import io.confluent.rest.impersonation.Errors;
import org.apache.hadoop.security.IdMappingServiceProvider;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.streams.KafkaClientSupplier;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.function.Function;

public class ByteProducerPool {
  private final Map<Integer, Producer<byte[], byte[]>> producers = new ConcurrentHashMap<>();
  private final Function<Integer, Producer<byte[], byte[]>> factory;
  private final IdMappingServiceProvider uidMapper;

  public ByteProducerPool(Map<String, Object> config,
                          KafkaClientSupplier clientSupplier,
                          IdMappingServiceProvider uidMapper) {
    this.factory = uid -> clientSupplier.getProducer(config);
    this.uidMapper = uidMapper;
  }

  public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
    try {
      final String userName = UserGroupInformation.getCurrentUser().getUserName();
      return producers.computeIfAbsent(uidMapper.getUid(userName), factory).send(record);
    } catch (Exception e) {
      throw Errors.serverLoginException(e);
    }
  }

  public void close() {
    for (Producer<byte[], byte[]> producer : producers.values()) {
      producer.close();
    }
  }
}
