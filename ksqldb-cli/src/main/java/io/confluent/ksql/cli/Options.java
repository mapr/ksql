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

package io.confluent.ksql.cli;

import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.ranges.LongRange;
import com.github.rvesse.airline.help.Help;
import com.github.rvesse.airline.parser.errors.ParseException;
import io.confluent.ksql.cli.console.OutputFormat;
import java.io.IOException;
import java.util.Optional;
import javax.inject.Inject;

@Command(name = "ksql", description = "KSQL CLI")
public class Options {

  private static final String STREAMED_QUERY_ROW_LIMIT_OPTION_NAME = "--query-row-limit";
  private static final String STREAMED_QUERY_TIMEOUT_OPTION_NAME = "--query-timeout";
  private static final String OUTPUT_FORMAT_OPTION_NAME = "--output";
  private static final String AUTH_METHOD_SHORT_OPTION = "-a";
  private static final String AUTH_METHOD_OPTION_NAME = "--auth";
  private static final String SSL_TRUSTSTORE_SHORT_OPTION_NAME = "-t";
  private static final String SSL_TRUSTSTORE_OPTION_NAME = "--truststore";
  private static final String SSL_TRUSTSTORE_PASSWORD_SHORT_OPTION_NAME = "-tp";
  private static final String SSL_TRUSTSTORE_PASSWORD_OPTION_NAME = "--truststore-password";
  private static final String SSL_TRUST_ALL_CERTS_OPTION_NAME = "--insecure";
  private static final String SSL_TRUST_ALL_CERTS_SHORT_OPTION_NAME = "-k";

  // Only here so that the help message generated by Help.help() is accurate
  @Inject
  public HelpOption<?> help;

  @SuppressWarnings({"unused", "FieldMayBeFinal", "FieldCanBeLocal"}) // Accessed via reflection
  @Once
  @Arguments(
      title = "server",
      description = "The address of the Ksql server to connect to (ex: http://confluent.io:9098)")
  private String server = "http://localhost:8084";

  private static final String CONFIGURATION_FILE_OPTION_NAME = "--config-file";

  @SuppressWarnings("unused") // Accessed via reflection
  @Option(
      name = CONFIGURATION_FILE_OPTION_NAME,
      description = "A file specifying configs for Ksql and its underlying Kafka Streams "
          + "instance(s). Refer to KSQL documentation for a list of available configs.")
  private String configFile;

  @Option(
      name = {AUTH_METHOD_OPTION_NAME, AUTH_METHOD_SHORT_OPTION},
      description =
          "If your KSQL server is configured for authentication, then provide your auth"
              + " method. The auth method must be specified separately with the "
              + AUTH_METHOD_OPTION_NAME
              + "/"
              + AUTH_METHOD_SHORT_OPTION
              + " flag",
      hidden = true)
  private String authMethod;

  @Option(
      name = {SSL_TRUSTSTORE_OPTION_NAME, SSL_TRUSTSTORE_SHORT_OPTION_NAME},
      description =
          "If your KSQL server is configured for 'https' protocol, "
              + "then you can provide your custom ssl_truststore"
              + " file. ssl_truststore file can be specified with the "
              + SSL_TRUSTSTORE_OPTION_NAME
              + "/"
              + SSL_TRUSTSTORE_SHORT_OPTION_NAME
              + " flag")
  private String sslTruststore;

  @Option(
      name = {SSL_TRUSTSTORE_PASSWORD_OPTION_NAME, SSL_TRUSTSTORE_PASSWORD_SHORT_OPTION_NAME},
      description =
          "If your KSQL server is configured for 'https' protocol, "
              + "then you can provide your custom ssl_truststore"
              + " file and password for it. The password can be specified with the "
              + SSL_TRUSTSTORE_PASSWORD_OPTION_NAME
              + "/"
              + SSL_TRUSTSTORE_PASSWORD_SHORT_OPTION_NAME
              + " flag")
  private String sslTruststorePassword;

  @Option(
      name = {SSL_TRUST_ALL_CERTS_OPTION_NAME, SSL_TRUST_ALL_CERTS_SHORT_OPTION_NAME},
      description =
          "Allows connections using 'https' without certs verification. "
              + "To enable it use "
              + SSL_TRUST_ALL_CERTS_OPTION_NAME
              + "/"
              + SSL_TRUST_ALL_CERTS_SHORT_OPTION_NAME
              + " flag")
  private Boolean sslTrustAllCertsEnable;

  @SuppressWarnings("unused") // Accessed via reflection
  @Option(
      name = STREAMED_QUERY_ROW_LIMIT_OPTION_NAME,
      description = "An optional maximum number of rows to read from streamed queries")
  @LongRange(
      min = 1)
  private Long streamedQueryRowLimit;

  @SuppressWarnings("unused") // Accessed via reflection
  @Option(
      name = STREAMED_QUERY_TIMEOUT_OPTION_NAME,
      description = "An optional time limit (in milliseconds) for streamed queries")
  @LongRange(
      min = 1)
  private Long streamedQueryTimeoutMs;

  @SuppressWarnings("FieldMayBeFinal") // Accessed via reflection
  @Option(
      name = OUTPUT_FORMAT_OPTION_NAME,
      description = "The output format to use "
          + "(either 'JSON' or 'TABULAR'; can be changed during REPL as well; "
          + "defaults to TABULAR)")
  private String outputFormat = OutputFormat.TABULAR.name();

  public static Options parse(final String...args) throws IOException {
    final SingleCommand<Options> optionsParser = SingleCommand.singleCommand(Options.class);

    // If just a help flag is given, an exception will be thrown due to missing required options;
    // hence, this workaround
    for (final String arg : args) {
      if ("--help".equals(arg) || "-h".equals(arg)) {
        Help.help(optionsParser.getCommandMetadata());
        return null;
      }
    }

    try {
      return optionsParser.parse(args);
    } catch (final ParseException exception) {
      if (exception.getMessage() != null) {
        System.err.println(exception.getMessage());
      } else {
        System.err.println("Options parsing failed for an unknown reason");
      }
      System.err.println("See the -h or --help flags for usage information");
    }
    return null;
  }

  public String getServer() {
    return server;
  }

  public Optional<String> getConfigFile() {
    return Optional.ofNullable(configFile);
  }

  public Long getStreamedQueryRowLimit() {
    return streamedQueryRowLimit;
  }

  public Long getStreamedQueryTimeoutMs() {
    return streamedQueryTimeoutMs;
  }

  public OutputFormat getOutputFormat() {
    return OutputFormat.valueOf(outputFormat);
  }

  public Optional<String> getAuthMethod() {
    return Optional.ofNullable(authMethod);
  }

  public Optional<String> getSslTruststore() {
    return Optional.ofNullable(sslTruststore);
  }

  public Optional<String> getSslTruststorePassword() {
    return Optional.ofNullable(sslTruststorePassword);
  }

  public Optional<Boolean> getSslTrustAllCertsEnable() {
    return Optional.ofNullable(sslTrustAllCertsEnable);
  }

}
