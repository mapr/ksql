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

package io.confluent.ksql.rest.filter;

import com.google.common.io.ByteStreams;
import io.confluent.ksql.rest.client.exception.AuthorizationException;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.filter.util.ByteConsumerPool;
import io.confluent.ksql.rest.filter.util.ByteProducerPool;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.rest.impersonation.ImpersonationUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.glassfish.jersey.server.ContainerRequest;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class AuthorizationFilter implements ContainerRequestFilter {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  private static final Logger logger = LoggerFactory.getLogger(AuthorizationFilter.class);

  private static final String INTERNAL_TOPIC = "ksql-authorization-auxiliary-topic";

  private final ByteConsumerPool byteConsumerPool;
  private final ByteProducerPool byteProducerPool;
  private final String internalTopic;

  public AuthorizationFilter(KsqlRestConfig ksqlRestConfig) {
    this.internalTopic = String.format("%s%s/ksql-commands:%s",
            KsqlConfig.KSQL_SERVICES_COMMON_FOLDER,
            new KsqlConfig(ksqlRestConfig.getKsqlConfigProperties())
                    .getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG),
            INTERNAL_TOPIC);
    this.byteConsumerPool = new ByteConsumerPool(getConsumerProperties(), internalTopic);
    this.byteProducerPool = new ByteProducerPool(getProducerProperties());
    this.initializeInternalTopicWithDummyRecord();
  }

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    final String authentication = requestContext.getHeaderString(HttpHeaders.AUTHORIZATION);
    final String cookie = retrieveCookie(requestContext);
    try {
      ImpersonationUtils.runAsUser(() -> {
        checkPermissions(requestContext);
        return null;
      }, authentication, cookie);
    } catch (Exception e) {
      final int errorCode = Response.Status.FORBIDDEN.getStatusCode();
      requestContext.abortWith(Response.status(Response.Status.FORBIDDEN)
              .entity(new KsqlErrorMessage(errorCode, e).toString())
              .build());
    }
  }

  private void initializeInternalTopicWithDummyRecord() {
    try {
      /** The method below is used to write initial record to INTERNAL_TOPIC.
       * It will not fail because authorization filter is created as cluster admin user.
       * Cluster admin user has appropriate permissions to send records to internal stream.
       */
      this.checkWritingPermissions();
    } catch (ExecutionException | InterruptedException e) {
      throw new KafkaException(e);
    }
  }

  private void checkPermissions(ContainerRequestContext requestContext) {
    try {
      final String path = ((ContainerRequest) requestContext).getPath(true);
      if (path.equals("ksql")) {
        final byte[] inputStream = ByteStreams.toByteArray(requestContext.getEntityStream());
        requestContext.setEntityStream(new ByteArrayInputStream(inputStream));
        final String jsonRequest = IOUtils.toString(new ByteArrayInputStream(inputStream));
        final JSONObject obj = new JSONObject(jsonRequest);
        final String request = obj.getString("ksql").toUpperCase();
        final boolean commandSet1 = request.startsWith("CREATE")
                || request.startsWith("RUN")
                || request.startsWith("DROP");
        final boolean commandSet2 = request.startsWith("TERMINATE") || request.startsWith("INSERT");
        if (commandSet1 || commandSet2) {
          checkWritingPermissions();
        } else {
          checkReadingPermissions();
        }
      } else {
        checkReadingPermissions();
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new AuthorizationException(
            "Access denied. This operation is not permitted for current user\n");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void checkWritingPermissions() throws ExecutionException, InterruptedException {
    final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(internalTopic, new byte[0]);
    byteProducerPool.send(record).get();
  }

  private void checkReadingPermissions() {
    final ConsumerRecords<byte[], byte[]> records = byteConsumerPool.poll();
    if (records.count() < 1) {
      throw new AuthorizationException(
              "Access denied. This operation is not permitted for current user\n");
    }
  }

  private String retrieveCookie(ContainerRequestContext requestContext) {
    final List<String> cookies = ((ContainerRequest) requestContext).getRequestHeader("Cookie");
    if (cookies != null) {
      return cookies.stream().filter(cookie -> cookie.startsWith("hadoop.auth"))
              .findFirst().orElse(null);
    }
    return null;
  }

  private Properties getProducerProperties() {
    final Properties properties = new Properties();
    properties.put(ProducerConfig.ACKS_CONFIG, "-1");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.ByteArraySerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.ByteArraySerializer.class);
    properties.put(ProducerConfig.RETRIES_CONFIG, 0);
    properties.put("streams.buffer.max.time.ms", "0");
    return properties;
  }

  private Properties getConsumerProperties() {
    final Properties properties = new Properties();
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
    return properties;
  }
}
