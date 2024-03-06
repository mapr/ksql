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

package io.confluent.ksql.rest.client;

import com.mapr.security.client.ClientSecurity;
import com.mapr.security.client.MapRClientSecurityException;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.io.Console;

public final class AuthenticationUtils {

  private AuthenticationUtils() {
    // hiding constructor
  }

  public static Pair<String, String> readUsernameAndPassword(final boolean sesssionExpired) {
    final Console console = System.console();

    if (sesssionExpired) {
      console.printf("Session is expired. Please, relogin...\n");
    }
    console.printf("Username: ");
    final String username = console.readLine();

    console.printf("Password: ");
    final char[] passwordChars = console.readPassword();
    final String password = new String(passwordChars);

    return new Pair<>(username, password);
  }

  public static String readChallengeString(final String clusterName) {
    final ClientSecurity cs = new ClientSecurity(clusterName);
    try {
      return cs.generateChallenge();
    } catch (MapRClientSecurityException e) {
      throw new KsqlException("Cannot read chalange string", e);
    }
  }
}
