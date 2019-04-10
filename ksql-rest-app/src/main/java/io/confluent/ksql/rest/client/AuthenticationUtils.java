package io.confluent.ksql.rest.client;

import com.mapr.security.client.ClientSecurity;
import com.mapr.security.client.MapRClientSecurityException;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;

import java.io.Console;

public class AuthenticationUtils {

  public static Pair<String, String> readUsernameAndPassword() {
    Console console = System.console();

    console.printf("Username: ");
    String username = console.readLine();

    console.printf("Password: ");
    char[] passwordChars = console.readPassword();
    String password = new String(passwordChars);

    return new Pair<>(username, password);
  }

  public static String readChallengeString() {
    ClientSecurity cs = new ClientSecurity();
    try {
      return cs.generateChallenge();
    } catch (MapRClientSecurityException e) {
      throw new KsqlException("Cannot read chalange string", e);
    }
  }
}
