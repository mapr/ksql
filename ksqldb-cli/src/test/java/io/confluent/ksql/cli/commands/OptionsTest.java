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

package io.confluent.ksql.cli.commands;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThrows;

import io.confluent.ksql.cli.Options;
import io.confluent.ksql.rest.client.BasicCredentials;
import java.util.Arrays;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

public class OptionsTest {

  @Test
  public void shouldUseDefaultServerIfNoneSupplied() {
    // When:
    final Options options = parse();

    // Then:
    assertThat(options.getServer(), is("http://localhost:8084"));
  }

  @Test
  public void shouldWorkWithUserSuppliedServer() {
    // When:
    final Options options = parse("custom server");

    // Then:
    assertThat(options.getServer(), is("custom server"));
  }

  @Test
  public void shouldThrowConfigExceptionIfOnlyApiKeyIsProvided() {
    // Given:
    final Options options = parse("--confluent-api-key", "api_key");

    // When:
    final Exception e = assertThrows(
        ConfigException.class,
        () -> options.getCCloudApiKey()
    );

    // Then:
    assertThat(e.getMessage(), containsString("You must specify both an API key and the associated secret"));
  }

  @Test
  public void shouldThrowConfigExceptionIfOnlyApiSecretIsProvided() {
    // Given:
    final Options options = parse("http://foobar", "--confluent-api-secret", "api_secret");

    // When:
    final Exception e = assertThrows(
        ConfigException.class,
        () -> options.getCCloudApiKey()
    );

    // Then:
    assertThat(e.getMessage(), containsString("You must specify both an API key and the associated secret"));
  }

  @Test
  public void shouldReturnApiKeySecretPairWhenBothProvided() {
    // When:
    final Options options = parse("http://foobar", "--confluent-api-key", "api_key", "--confluent-api-secret", "api_secret");

    // Then:
    assertThat(options.getCCloudApiKey(),
        is(Optional.of(BasicCredentials.of("api_key", "api_secret"))));
  }

  @Test
  public void shouldReturnEmptyOptionWhenApiKeyNotPresent() {
    // When:
    final Options options = parse();

    // Then:
    assertThat(options.getCCloudApiKey(), is(Optional.empty()));
  }

  @Test
  public void shouldDefineVariables() {
    // When:
    final Options options = parse("-d", "env=qa", "-d", "size=1", "--define", "prod=true");

    // Then:
    assertThat(options.getVariables().size(), is(3));
    assertThat(options.getVariables(), hasEntry("env", "qa"));
    assertThat(options.getVariables(), hasEntry("size", "1"));
    assertThat(options.getVariables(), hasEntry("prod", "true"));
  }


  private static Options parse(final String... args) {
    try {
      final Options parsed = Options.parse(args);
      assertThat(parsed, is(notNullValue()));
      return parsed;
    } catch (final Exception e) {
      throw new AssertionError("Failed to parse options: " + StringUtils.join(args, ","), e);
    }
  }
}
