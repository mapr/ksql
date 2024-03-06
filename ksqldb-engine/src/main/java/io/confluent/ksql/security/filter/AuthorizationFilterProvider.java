/**
 * <p>>Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at</p>
 *
 *     <p>http://www.apache.org/licenses/LICENSE-2.0</p>
 *
 * <p>>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.</p>
 */

package io.confluent.ksql.security.filter;

import io.confluent.ksql.security.filter.util.ByteConsumerPool;
import io.confluent.ksql.security.filter.util.ByteProducerPool;
import io.confluent.ksql.security.filter.util.UnixUserIdUtils;
import io.confluent.ksql.util.KsqlConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.security.IdMappingServiceProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;

public final class AuthorizationFilterProvider {
  private AuthorizationFilterProvider() {
  }

  private static final String INTERNAL_TOPIC = "ksql-authorization-auxiliary-topic";

  public static AuthorizationFilter configure(final KsqlConfig ksqlConfig) {
    final KafkaClientSupplier clientSupplier = new DefaultKafkaClientSupplier();
    final IdMappingServiceProvider idMapper = UnixUserIdUtils.getUnixIdMapper();
    final ByteConsumerPool consumerPool = createConsumerPool(clientSupplier, idMapper);
    final ByteProducerPool producerPool = createProducerPool(clientSupplier, idMapper);
    final String internalTopic = getConfiguredAuxiliaryTopic(ksqlConfig);
    final AuthorizationFilter filter = new AuthorizationFilter(
        consumerPool, producerPool, internalTopic
    );
    filter.initialize();
    return filter;
  }

  private static String getConfiguredAuxiliaryTopic(final KsqlConfig ksqlConfig) {
    return String.format("%s%s/ksql-commands:%s",
                         KsqlConfig.KSQL_SERVICES_COMMON_FOLDER,
                         ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG),
                         INTERNAL_TOPIC);
  }

  private static ByteProducerPool createProducerPool(final KafkaClientSupplier clientSupplier,
                                                     final IdMappingServiceProvider idMapper) {
    final Map<String, Object> properties = new HashMap<>();
    properties.put(ProducerConfig.ACKS_CONFIG, "-1");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    properties.put(ProducerConfig.RETRIES_CONFIG, 0);
    properties.put("streams.buffer.max.time.ms", "0");
    return new ByteProducerPool(properties, clientSupplier, idMapper);
  }

  private static ByteConsumerPool createConsumerPool(final KafkaClientSupplier clientSupplier,
                                                     final IdMappingServiceProvider idMapper) {
    final Map<String, Object> properties = new HashMap<>();
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    return new ByteConsumerPool(properties, clientSupplier, idMapper);
  }

}
