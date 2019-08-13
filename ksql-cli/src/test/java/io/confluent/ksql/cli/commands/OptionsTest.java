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

package io.confluent.ksql.cli.commands;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.confluent.ksql.cli.Options;
import org.junit.Test;

public class OptionsTest {

  @Test
  public void shouldReturnAuthMethodWhenProvided() throws Exception {
    final Options options = Options.parse("http://foobar", "-a", "maprsasl");
    assertTrue(options.getAuthMethod().isPresent());
  }

  @Test
  public void shouldReturnEmptyOptionWhenAuthMethodNotPresent() throws Exception {
    final Options options = Options.parse("http://foobar");
    assertFalse(options.getAuthMethod().isPresent());
  }

}
