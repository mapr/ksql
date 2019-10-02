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
import io.confluent.ksql.rest.util.ByteConsumerPool;
import io.confluent.ksql.rest.util.ByteProducerPool;
import io.confluent.rest.auth.MaprAuthenticationUtils;
import io.confluent.rest.impersonation.ImpersonationUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.glassfish.jersey.server.ContainerRequest;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class AuthorizationFilter implements ContainerRequestFilter {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  private static final Logger logger = LoggerFactory.getLogger(AuthorizationFilter.class);

  private static final String[] commandWithWritePermsPrefixes =
      {"CREATE", "DROP", "RUN", "TERMINATE", "INSERT"};

  private final ByteConsumerPool byteConsumerPool;
  private final ByteProducerPool byteProducerPool;
  private final String internalTopic;

  public AuthorizationFilter(ByteConsumerPool byteConsumerPool,
                             ByteProducerPool byteProducerPool,
                             String internalTopic) {
    this.byteConsumerPool = byteConsumerPool;
    this.byteProducerPool = byteProducerPool;
    this.internalTopic = internalTopic;
  }

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    try {
      final String user = MaprAuthenticationUtils.getUserNameFromRequestContext(requestContext);
      ImpersonationUtils.executor().runThrowingAs(user, () -> {
        checkPermissions(requestContext);
        return null;
      });
    } catch (NotImplementedException e) {
      final int errorCode = Response.Status.NOT_IMPLEMENTED.getStatusCode();
      requestContext.abortWith(Response.status(Response.Status.NOT_IMPLEMENTED)
              .entity(new KsqlErrorMessage(errorCode, e.getMessage()))
              .build());
    } catch (Exception e) {
      final int errorCode = Response.Status.FORBIDDEN.getStatusCode();
      requestContext.abortWith(Response.status(Response.Status.FORBIDDEN)
              .entity(new KsqlErrorMessage(errorCode, e.getMessage()))
              .build());
    }
  }

  public void initialize() {
    /** The method below is used to write initial record to INTERNAL_TOPIC.
     * It will not fail because authorization filter is created as cluster admin user.
     * Cluster admin user has appropriate permissions to send records to internal stream.
     */
    try {
      this.checkWritingPermissions();
    } catch (Exception e) {
      throw new KafkaException(e);
    }
  }

  private void checkPermissions(ContainerRequestContext requestContext) throws IOException {
    final String path = ((ContainerRequest) requestContext).getPath(true);

    if (path.equals("info")) {
      return;
    } else if (path.startsWith("ksql")) {
      checkPermsForKsqlPath(requestContext);
    } else if (path.equals("")
            || path.startsWith("query")
            || path.startsWith("status")) {
      checkReadingPermissions();
    } else {
      throw new NotImplementedException(
              "KSQL Authorization Filter internal error: path /"
                      + path
                      + " is not supported by authorization filter\n");
    }
  }

  private void checkPermsForKsqlPath(ContainerRequestContext requestContext) throws IOException {
    final byte[] inputStream = ByteStreams.toByteArray(requestContext.getEntityStream());
    requestContext.setEntityStream(new ByteArrayInputStream(inputStream));
    final String jsonRequest = IOUtils.toString(new ByteArrayInputStream(inputStream), "UTF-8");
    final JSONObject obj = new JSONObject(jsonRequest);
    final String command = obj.getString("ksql").toUpperCase().trim();

    if (commandRequiresWritingPerms(command)) {
      checkWritingPermissions();
    } else {
      checkReadingPermissions();
    }
  }

  private boolean commandRequiresWritingPerms(String command) {
    for (String commandWithWritePermsPrefix : commandWithWritePermsPrefixes) {
      if (command.startsWith(commandWithWritePermsPrefix)) {
        return true;
      }
    }
    return false;
  }

  private void checkWritingPermissions() throws IOException {
    try {
      final ProducerRecord<byte[], byte[]> record =
              new ProducerRecord<>(internalTopic, new byte[0]);
      byteProducerPool.send(record).get();
    } catch (Exception e) {
      logger.debug("Forbidden through failed send operation", e);
      throw forbidden("modify");
    }
  }

  private void checkReadingPermissions() throws IOException {
    final ConsumerRecords<byte[], byte[]> records;
    try {
      records = byteConsumerPool.poll(internalTopic);
    } catch (Exception e) {
      logger.debug("Forbidden through failed poll operation", e);
      throw forbidden("read");
    }

    if (records.count() < 1) {
      logger.debug("Forbidden through missing polled records");
      throw forbidden("read");
    }
  }

  private AuthorizationException forbidden(String permission) throws IOException {
    final String currentUser = UserGroupInformation.getCurrentUser().getUserName();
    return new AuthorizationException("FORBIDDEN. User " + currentUser
            + " doesn't have permission to " + permission
            + " to execute the operation");
  }
}
