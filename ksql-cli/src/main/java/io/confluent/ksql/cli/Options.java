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

package io.confluent.ksql.cli;

import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.Parser;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.github.rvesse.airline.annotations.restrictions.global.NoMissingOptionValues;
import com.github.rvesse.airline.annotations.restrictions.global.NoUnexpectedArguments;
import com.github.rvesse.airline.annotations.restrictions.ranges.LongRange;
import com.github.rvesse.airline.parser.options.StandardOptionParser;
import io.confluent.ksql.cli.console.OutputFormat;
import io.confluent.ksql.rest.util.OptionsParser;
import java.io.IOException;
import java.util.Optional;
import javax.inject.Inject;

@Command(name = "ksql", description = "KSQL CLI")
@NoUnexpectedArguments
@NoMissingOptionValues
@Parser(useDefaultOptionParsers = false, optionParsers = { StandardOptionParser.class })
public class Options {

  private static final String STREAMED_QUERY_ROW_LIMIT_OPTION_NAME = "--query-row-limit";
  private static final String STREAMED_QUERY_TIMEOUT_OPTION_NAME = "--query-timeout";
  private static final String OUTPUT_FORMAT_OPTION_NAME = "--output";
  private static final String AUTH_METHOD_SHORT_OPTION = "-a";
  private static final String AUTH_METHOD_OPTION_NAME = "--auth";

  // Only here so that the help message generated by Help.help() is accurate
  @Inject
  public HelpOption help;

  @Once
  @Required
  @Arguments(
      title = "server",
      description = "The address of the Ksql server to connect to (ex: http://confluent.io:9098)")
  private String server;

  private static final String CONFIGURATION_FILE_OPTION_NAME = "--config-file";

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
      name = STREAMED_QUERY_ROW_LIMIT_OPTION_NAME,
      description = "An optional maximum number of rows to read from streamed queries")

  @LongRange(
      min = 1)
  private Long streamedQueryRowLimit;

  @Option(
      name = STREAMED_QUERY_TIMEOUT_OPTION_NAME,
      description = "An optional time limit (in milliseconds) for streamed queries")
  @LongRange(
      min = 1)
  private Long streamedQueryTimeoutMs;

  @Option(
      name = OUTPUT_FORMAT_OPTION_NAME,
      description = "The output format to use "
          + "(either 'JSON' or 'TABULAR'; can be changed during REPL as well; "
          + "defaults to TABULAR)")
  private String outputFormat = OutputFormat.TABULAR.name();

  public static Options parse(final String...args) throws IOException {
    return OptionsParser.parse(args, Options.class);
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

  public void setAuthMethod(final String authMethod) {
    this.authMethod = authMethod;
  }

}
