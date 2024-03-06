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

package io.confluent.ksql.security.filter.util;

import io.confluent.rest.impersonation.Errors;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.function.Function;
import org.apache.hadoop.security.IdMappingServiceProvider;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.streams.KafkaClientSupplier;

public class ByteProducerPool {
  private final Map<Integer, Producer<byte[], byte[]>> producers = new ConcurrentHashMap<>();
  private final Function<Integer, Producer<byte[], byte[]>> factory;
  private final IdMappingServiceProvider uidMapper;

  public ByteProducerPool(final Map<String, Object> config,
                          final KafkaClientSupplier clientSupplier,
                          final IdMappingServiceProvider uidMapper) {
    this.factory = uid -> clientSupplier.getProducer(config);
    this.uidMapper = uidMapper;
  }

  public Future<RecordMetadata> send(final ProducerRecord<byte[], byte[]> record) {
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
