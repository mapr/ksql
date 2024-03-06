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

package io.confluent.ksql.security.filter;

import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.DropStatement;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.TerminateQuery;
import io.confluent.ksql.security.KsqlAuthorizationValidator;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.security.filter.util.ByteConsumerPool;
import io.confluent.ksql.security.filter.util.ByteProducerPool;
import io.confluent.ksql.topic.SourceTopicsExtractor;
import io.confluent.rest.impersonation.ImpersonationUtils;
import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.acl.AclOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AuthorizationFilter implements KsqlAuthorizationValidator {

  private static final Logger logger = LoggerFactory.getLogger(AuthorizationFilter.class);

  private final ByteConsumerPool byteConsumerPool;
  private final ByteProducerPool byteProducerPool;
  private final String internalTopic;

  public AuthorizationFilter(final ByteConsumerPool byteConsumerPool,
                             final ByteProducerPool byteProducerPool,
                             final String internalTopic) {
    this.byteConsumerPool = byteConsumerPool;
    this.byteProducerPool = byteProducerPool;
    this.internalTopic = internalTopic;
  }

  @Override
  public void checkAuthorization(final KsqlSecurityContext securityContext,
                                 final MetaStore metaStore,
                                 final Statement statement) {

    try {
      if (securityContext.getUserPrincipal().isPresent()) {
        final String user = securityContext.getUserPrincipal().get().getName();
        ImpersonationUtils.executor().runThrowingAs(user, () -> {
          checkPermissions(statement);
          return null;
        });
      }
    } catch (Exception e) {
      final Set<String> topics = new SourceTopicsExtractor(metaStore).getSourceTopics().stream()
              .map(KsqlTopic::getKafkaTopicName).collect(Collectors.toSet());
      logger.debug("Exception:", e);
      throw new KsqlTopicAuthorizationException(AclOperation.UNKNOWN, topics);
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

  private void checkPermissions(final Statement statement) throws IOException {

    // CHECKSTYLE_RULES.OFF: BooleanExpressionComplexity
    if (statement instanceof InsertInto
        || statement instanceof DropStatement
        || statement instanceof TerminateQuery
        || statement instanceof CreateAsSelect
        || statement instanceof CreateSource) {
      // CHECKSTYLE_RULES.ON: BooleanExpressionComplexity
      checkWritingPermissions();
    } else {
      checkReadingPermissions();
    }
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

  private AuthorizationException forbidden(final String permission) throws IOException {
    final String currentUser = UserGroupInformation.getCurrentUser().getUserName();
    return new AuthorizationException("FORBIDDEN. User " + currentUser
        + " doesn't have permission to " + permission
        + " to execute the operation");
  }

}
