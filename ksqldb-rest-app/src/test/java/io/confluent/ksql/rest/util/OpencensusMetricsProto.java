/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.util;

import io.opencensus.proto.metrics.v1.Metric;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class OpencensusMetricsProto implements Serde<Metric>, Serializer<Metric>, Deserializer<Metric> {
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {

  }

  @Override
  public Metric deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }
    try {
      return Metric.parseFrom(data);
    } catch (SerializationException e) {
      throw e;
    } catch (Exception e) {
      String errMsg = "Error deserializing protobuf message";
      throw new SerializationException(errMsg, e);
    }
  }

  @Override
  public Metric deserialize(String topic, Headers headers, byte[] data) {
    return deserialize(topic, data);
  }

  @Override
  public byte[] serialize(String topic, Metric data) {
    return data.toByteArray();
  }

  @Override
  public byte[] serialize(String topic, Headers headers, Metric data) {
    return serialize(topic, data);
  }

  @Override
  public void close() {

  }

  @Override
  public Serializer<Metric> serializer() {
    return this;
  }

  @Override
  public Deserializer<Metric> deserializer() {
    return this;
  }


}