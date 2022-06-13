/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql;

import com.google.common.annotations.VisibleForTesting;
import com.mapr.baseutils.cldbutils.CLDBRpcCommonUtils;
import com.mapr.web.security.SslConfig;
import com.mapr.web.security.WebSecurityManager;
import io.confluent.ksql.cli.Cli;
import io.confluent.ksql.cli.Options;
import io.confluent.ksql.cli.console.OutputFormat;
import io.confluent.ksql.properties.PropertiesUtil;
import io.confluent.ksql.rest.client.BasicCredentials;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.KsqlRestClientException;
import io.confluent.ksql.util.ErrorMessageUtil;
import io.confluent.ksql.version.metrics.KsqlVersionCheckerAgent;
import io.confluent.ksql.version.metrics.collector.KsqlModuleType;
import io.confluent.rest.RestConfig;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Predicate;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Ksql {
  private static final Logger LOGGER = LoggerFactory.getLogger(Ksql.class);
  private static final Predicate<String> NOT_CLIENT_SIDE_CONFIG = key -> !key.startsWith("ssl.");

  private final Options options;
  private final KsqlClientBuilder clientBuilder;
  private final Properties systemProps;
  private final CliBuilder cliBuilder;

  @VisibleForTesting
  Ksql(
      final Options options,
      final Properties systemProps,
      final KsqlClientBuilder clientBuilder,
      final CliBuilder cliBuilder
  ) {
    this.options = Objects.requireNonNull(options, "options");
    this.systemProps = Objects.requireNonNull(systemProps, "systemProps");
    this.clientBuilder = Objects.requireNonNull(clientBuilder, "clientBuilder");
    this.cliBuilder = Objects.requireNonNull(cliBuilder, "cliBuilder");
  }

  public static void main(final String[] args) throws IOException {
    final boolean secureCluster = UserGroupInformation.isSecurityEnabled();
    final String defaultKsqlServerUrl = secureCluster ? "https://localhost:8084"
        : "http://localhost:8084";
    final Options options = args.length == 0 ? Options.parse(defaultKsqlServerUrl)
        : Options.parse(args);

    if (options == null) {
      System.exit(-1);
    }

    if (secureCluster && !options.getAuthMethod().isPresent()) {
      options.setAuthMethod("maprsasl");
    }

    if (options.getClusterName() != null) {
      CLDBRpcCommonUtils.getInstance().setCurrentClusterName(options.getClusterName());
    }

    try {
      new Ksql(options, System.getProperties(), KsqlRestClient::create, Cli::build).run();
    } catch (final Exception e) {
      final String msg = ErrorMessageUtil.buildErrorMessage(e);
      LOGGER.error(msg);
      System.err.println(msg);
      System.exit(-1);
    }
  }

  void run() {
    final Map<String, String> configProps = options.getConfigFile()
        .map(Ksql::loadProperties)
        .orElseGet(Collections::emptyMap);

    try (KsqlRestClient restClient = buildClient(configProps)) {

      final KsqlVersionCheckerAgent versionChecker = new KsqlVersionCheckerAgent(() -> false);
      versionChecker.start(KsqlModuleType.CLI, PropertiesUtil.asProperties(configProps));
      try (Cli cli = cliBuilder.build(
          options.getStreamedQueryRowLimit(),
          options.getStreamedQueryTimeoutMs(),
          options.getOutputFormat(),
          restClient)
      ) {
        cli.runInteractively();
      }
    }
  }

  private KsqlRestClient buildClient(
      final Map<String, String> configProps
  ) {
    final Map<String, String> localProps = stripClientSideProperties(configProps);
    final Map<String, String> clientProps = PropertiesUtil.applyOverrides(configProps, systemProps);
    final String server = options.getServer();
    final Optional<String> authMethod = options.getAuthMethod();
    final String clusterName = options.getClusterName();
    final Optional<BasicCredentials> creds = Optional.empty();

    return clientBuilder.build(server, localProps,
        updateClientSslWithDefaultsIfNeeded(clientProps),creds, authMethod, clusterName);
  }

  private Map<String, String> updateClientSslWithDefaultsIfNeeded(final Map<String, String> props) {
    final Map<String, String> updatedProps = new HashMap<>(props);
    if (!props.containsKey(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)) {
      final SslConfig sslConfig = WebSecurityManager.getSslConfig();
      updatedProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
          sslConfig.getClientTruststoreLocation());
      updatedProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
          String.valueOf(sslConfig.getClientTruststorePassword()));
      updatedProps.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
          sslConfig.getClientTruststoreType().toUpperCase());
    }
    if (options.getSslTrustAllCertsEnable().isPresent()) {
      updatedProps.put(RestConfig.SSL_TRUSTALLCERTS_CONFIG,
          options.getSslTrustAllCertsEnable().get().toString());
    }

    if (options.getSslTruststore().isPresent()) {
      updatedProps.put(RestConfig.SSL_TRUSTSTORE_LOCATION_CONFIG,
          options.getSslTruststore().get());
      if (options.getSslTruststorePassword().isPresent()) {
        updatedProps.put(RestConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG,
            options.getSslTruststorePassword().get());
      }
    } else if (options.getSslTruststorePassword().isPresent()) {
      throw new KsqlRestClientException("SSL truststore is not specified, "
          + "but truststore password is specified. Truststore cannot be blank.");
    }
    return Collections.unmodifiableMap(updatedProps);
  }

  private static Map<String, String> stripClientSideProperties(final Map<String, String> props) {
    return PropertiesUtil.filterByKey(props, NOT_CLIENT_SIDE_CONFIG);
  }

  private static Map<String, String> loadProperties(final String propertiesFile) {
    return PropertiesUtil.loadProperties(new File(propertiesFile));
  }

  interface KsqlClientBuilder {
    KsqlRestClient build(
        String serverAddress,
        Map<String, ?> localProperties,
        Map<String, String> clientProps,
        Optional<BasicCredentials> creds,
        Optional<String> challengeString,
        String clusterName);
  }

  interface CliBuilder {
    Cli build(
        Long streamedQueryRowLimit,
        Long streamedQueryTimeoutMs,
        OutputFormat outputFormat,
        KsqlRestClient restClient);
  }
}
