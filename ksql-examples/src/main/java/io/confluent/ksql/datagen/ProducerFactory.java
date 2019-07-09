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

package io.confluent.ksql.datagen;

import io.confluent.ksql.datagen.DataGen.Arguments.Format;
import io.confluent.ksql.util.KsqlConfig;

import java.util.HashMap;
import java.util.Map;

class ProducerFactory {
  DataGenProducer getProducer(final Format format,
                              final String schemaRegistryUrl,
                              final Map<?,?> dataGenProperties) {
    switch (format) {
      case AVRO:
        final Map<Object,Object> ksqlConfig = new HashMap<>();
        ksqlConfig.put(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, schemaRegistryUrl);
        ksqlConfig.putAll(dataGenProperties);
        return new AvroProducer(new KsqlConfig(ksqlConfig));

      case JSON:
        return new JsonProducer();

      case DELIMITED:
        return new DelimitedProducer();

      default:
        throw new IllegalArgumentException("Invalid format in '" + format
            + "'; was expecting one of AVRO, JSON, or DELIMITED%n");
    }
  }
}
