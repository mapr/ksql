/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.services;

import io.confluent.ksql.links.DocumentationLinks;
import io.confluent.ksql.util.KsqlServerException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KafkaClusterUtil {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaClusterUtil.class);

  private static final long DESCRIBE_CLUSTER_TIMEOUT_SECONDS = 30;

  private KafkaClusterUtil() {

  }

  public static boolean isAuthorizedOperationsSupported(final Admin adminClient) {
    try {
      final DescribeClusterResult authorizedOperations = adminClient.describeCluster(
          new DescribeClusterOptions().includeAuthorizedOperations(true)
      );

      return authorizedOperations.authorizedOperations().get() != null;
    } catch (final UnsupportedVersionException e) {
      return false;
    } catch (final Exception e) {
      throw new KsqlServerException("Could not get Kafka authorized operations!", e);
    }
  }

  public static Config getConfig(final Admin adminClient) {
    try {
      return new Config(Collections.<ConfigEntry>emptySet());
    } catch (final KsqlServerException e) {
      throw e;
    } catch (final ClusterAuthorizationException e) {
      throw new KsqlServerException("Could not get Kafka cluster configuration. "
          + "Please ensure the ksql principal has " + AclOperation.DESCRIBE_CONFIGS + " rights "
          + "on the Kafka cluster."
          + System.lineSeparator()
          + "See " + DocumentationLinks.SECURITY_REQUIRED_ACLS_DOC_URL + " for more info.",
          e
      );
    } catch (final Exception e) {
      throw new KsqlServerException("Could not get Kafka cluster configuration!", e);
    }
  }

  public static String getKafkaClusterId(final ServiceContext serviceContext) {
    try {
      return serviceContext.getAdminClient()
          .describeCluster()
          .clusterId()
          .get(DESCRIBE_CLUSTER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (final Exception e) {
      throw new RuntimeException("Failed to get Kafka cluster information", e);
    }
  }
}
