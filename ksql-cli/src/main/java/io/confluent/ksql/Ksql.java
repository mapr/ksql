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

package io.confluent.ksql;

import io.confluent.ksql.cli.Cli;
import io.confluent.ksql.cli.Options;
import io.confluent.ksql.cli.console.JLineTerminal;
import io.confluent.ksql.rest.client.AuthenticationUtils;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.util.ErrorMessageUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.version.metrics.KsqlVersionCheckerAgent;
import io.confluent.ksql.version.metrics.collector.KsqlModuleType;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public final class Ksql {
  private static final Logger LOGGER = LoggerFactory.getLogger(Ksql.class);

  private Ksql() {
  }

  public static void main(final String[] args) throws IOException {
    final boolean secureCluster = UserGroupInformation.isSecurityEnabled();
    final String defaultKsqlServerUrl = secureCluster ? "https://localhost:8084"
                                                      : "http://localhost:8084";
    final Options options = args.length == 0 ? Options.parse(defaultKsqlServerUrl)
                                             : Options.parse(args);
    if (secureCluster && !options.getAuthMethod().isPresent()) {
      options.setAuthMethod("maprsasl");
    }
    if (options == null) {
      System.exit(-1);
    }

    try {
      final Properties properties = loadProperties(options.getConfigFile());
      final KsqlRestClient restClient = new KsqlRestClient(options.getServer(), properties);
      final Optional<String> authMethod = options.getAuthMethod();

      authMethod.ifPresent(method -> {
        if (method.equals("basic")) {
          final Pair<String, String> credentials = AuthenticationUtils.readUsernameAndPassword();
          restClient.setupAuthenticationCredentials(credentials.left, credentials.right);
        }
        if (method.equals("maprsasl")) {
          final String readChallangeString = AuthenticationUtils.readChallengeString();
          restClient.setChallengeStringForAuthentication(readChallangeString);
        }
      });

      final KsqlVersionCheckerAgent versionChecker = new KsqlVersionCheckerAgent(() -> false);
      versionChecker.start(KsqlModuleType.CLI, properties);

      try (Cli cli = new Cli(options.getStreamedQueryRowLimit(),
                                   options.getStreamedQueryTimeoutMs(),
                                   restClient,
                                   new JLineTerminal(options.getOutputFormat(), restClient))
      ) {
        cli.runInteractively();
      }
    } catch (final Exception e) {
      final String msg = ErrorMessageUtil.buildErrorMessage(e);
      LOGGER.error(msg);
      System.err.println(msg);
      System.exit(-1);
    }
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private static Properties loadProperties(final Optional<String> propertiesFile) {
    final Properties properties = new Properties();
    propertiesFile.ifPresent(file -> {
      try (FileInputStream input = new FileInputStream(file)) {
        properties.load(input);
        if (properties.containsKey(KsqlConfig.KSQL_SERVICE_ID_CONFIG)) {
          properties.put(
              StreamsConfig.APPLICATION_ID_CONFIG,
              properties.getProperty(KsqlConfig.KSQL_SERVICE_ID_CONFIG)
          );
        }
      } catch (final IOException e) {
        throw new KsqlException("failed to load properties file: " + file, e);
      }
    });
    return properties;
  }

}
