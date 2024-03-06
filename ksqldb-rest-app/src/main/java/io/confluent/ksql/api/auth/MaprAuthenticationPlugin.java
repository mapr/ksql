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

package io.confluent.ksql.api.auth;

import static com.mapr.security.Security.Decrypt;
import static com.mapr.security.Security.DecryptTicket;
import static io.confluent.ksql.rest.Errors.ERROR_CODE_SERVER_ERROR;
import static io.confluent.ksql.rest.Errors.ERROR_CODE_UNAUTHORIZED;

import com.mapr.fs.proto.Security.AuthenticationReqFull;
import com.mapr.fs.proto.Security.AuthenticationResp;
import com.mapr.fs.proto.Security.AuthenticationResp.Builder;
import com.mapr.fs.proto.Security.CredentialsMsg;
import com.mapr.fs.proto.Security.Key;
import com.mapr.fs.proto.Security.Ticket;
import com.mapr.security.ClusterServerTicketGeneration;
import com.mapr.security.MapRPrincipal;
import com.mapr.security.MutableInt;
import com.mapr.security.Security;
import com.mapr.security.maprauth.MaprAuthenticator;
import io.confluent.ksql.api.server.KsqlApiException;
import io.confluent.rest.RestConfig;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.Cookie;
import io.vertx.ext.web.RoutingContext;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.authentication.util.RandomSignerSecretProvider;
import org.apache.hadoop.security.authentication.util.Signer;
import org.apache.hadoop.security.authentication.util.SignerException;
import org.apache.hadoop.security.authentication.util.SignerSecretProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapR environment authentication plugin to be embedded into vertx environment.
 * This is an attempt to reimplement
 * org.apache.hadoop.security.authentication.server.AuthenticationFilter and
 * org.apache.hadoop.security.authentication.server.MultiMechsAuthenticationHandler to be used
 * in vertx. It also
 * implements io.confluent.rest.util.HeadersFilter to add security headers from headers.file.
 * So in general this class performs the following:
 * 1. MapR-SASL authentication
 * 2. BASIC authentication
 * 3. "hadoop.auth" cookie handling
 * 4. Adding security headers from "headers.file".
 * (note that there is no kerberos auth implementation)
 * For more information please refer to classes mentioned above (from hadoop-common and
 * rest-utils projects).
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class MaprAuthenticationPlugin implements AuthenticationPlugin {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  private static final Logger LOG = LoggerFactory.getLogger(MaprAuthenticationPlugin.class);

  private Signer signer;
  //only random secret provider supported
  private SignerSecretProvider secretProvider = new RandomSignerSecretProvider();
  private long validity = 7200 * 1000L;
  private String cookieDomain;
  private String cookiePath;
  private final Properties securityHeaders = new Properties();

  public static final String AUTHENTICATION_COOKIE_EXPIRATION_CONFIG =
      "authentication.cookie.expiration";

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  // CHECKSTYLE_RULES.OFF: NPathComplexity
  @Override
  public void configure(final Map<String, ?> map) {
    // Load security headers map from headers.file
    final String headersFilename = (String)map.get(RestConfig.HEADERS_FILE_CONFIG);
    final File headersFile = new File(headersFilename);
    try {
      securityHeaders.loadFromXML(FileUtils.openInputStream(headersFile));
    } catch (IOException e) {
      throw new KsqlApiException(e.getMessage(), ERROR_CODE_SERVER_ERROR);
    }

    // MaprSasl server-side part should be initiated manually
    if (UserGroupInformation.isSecurityEnabled()) {
      try {
        ClusterServerTicketGeneration.getInstance().generateTicketAndSetServerKey();
      } catch (IOException e) {
        throw new KsqlApiException(e.getMessage(), ERROR_CODE_SERVER_ERROR);
      }
    }
    try {
      if (map.containsKey(AUTHENTICATION_COOKIE_EXPIRATION_CONFIG)) {
        validity = Long.parseLong(map.get("authentication.cookie.expiration").toString()) * 1000L;
      }
      this.secretProvider.init(null, null, this.validity);
      LOG.info("Initialized secret provider " + this.secretProvider.getClass().getName()
          + " with validity=" + this.validity);
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        e.printStackTrace();
      }
      throw new KsqlApiException(
          "Unable to initialize a secret provider. " + e.getMessage(), ERROR_CODE_SERVER_ERROR);
    }
    this.signer = new Signer(this.secretProvider);
    String domainName = null;

    try {
      final InetAddress localHost = InetAddress.getLocalHost();
      final String fqdn = localHost.getCanonicalHostName();
      if (fqdn != null && !fqdn.isEmpty() && fqdn.contains(".")) {
        domainName = fqdn.substring(fqdn.indexOf("."));
      }
    } catch (UnknownHostException e) {
      // NOP
    }
    this.cookieDomain = domainName;
    this.cookiePath = "/";
  }

  // AuthenticationFilter.getToken() reimplemented
  private AuthenticationToken getToken(final RoutingContext routingContext)
      throws AuthenticationException {
    AuthenticationToken token = null;
    String tokenStr = null;

    final Map<String, Cookie> cookies = routingContext.cookieMap();
    if (cookies.containsKey("hadoop.auth")) {
      tokenStr = cookies.get("hadoop.auth").getValue();
      try {
        tokenStr = this.signer.verifyAndExtract(tokenStr);
      } catch (SignerException e) {
        throw new AuthenticationException(e);
      }
    }

    if (tokenStr != null) {
      token = AuthenticationToken.parse(tokenStr);
      if (!token.getType().equals("maprauth") && !token.getType().equals("basic")) {
        throw new AuthenticationException("Invalid AuthenticationToken type");
      }

      if (token.isExpired()) {
        throw new AuthenticationException("AuthenticationToken expired");
      }
    }
    return token;
  }

  // MaprAuthenticationHandler.maprAuthenticate() reimplmented
  // CHECKSTYLE_RULES.OFF: JavaNCSS
  private AuthenticationToken maprAuthenticate(final RoutingContext routingContext) {
    // CHECKSTYLE_RULES.ON: JavaNCSS

    String authorization = routingContext.request().getHeader(KerberosAuthenticator.AUTHORIZATION);

    /* Sanity check: Make sure header contains Mapr negotiate */
    if (authorization == null || !authorization.startsWith(MaprAuthenticator.NEGOTIATE)) {
      routingContext.fail(401,
          new KsqlApiException("Unauthorized", ERROR_CODE_UNAUTHORIZED));
      return null;
    } else {
      authorization = authorization.substring(MaprAuthenticator.NEGOTIATE.length()).trim();
      try {
        final byte[] base64decoded = Base64.decodeBase64(authorization);

        LOG.trace("MaprAuthentication is started");
        final AuthenticationReqFull req = AuthenticationReqFull.parseFrom(base64decoded);

        if (req != null && req.getEncryptedTicket() != null) {
          final byte [] encryptedTicket =
              req.getEncryptedTicket().toByteArray();
          final MutableInt err = new MutableInt();

          /* During login ServerKey should have been set -
           * if it was not set we have a wrong server here
           */
          final Ticket decryptedTicket = DecryptTicket(encryptedTicket, err);

          if (err.GetValue() != 0 || decryptedTicket == null) {
            final String decryptError = "Error while decrypting ticket and key " + err.GetValue();
            routingContext.response().putHeader(
                MaprAuthenticator.WWW_ERR_AUTHENTICATE, decryptError);
            routingContext.fail(401,
                new KsqlApiException("Unauthorized", ERROR_CODE_UNAUTHORIZED));
            return null;
          }

          final CredentialsMsg userCreds = decryptedTicket.getUserCreds();
          final Key userKey = decryptedTicket.getUserKey();
          final String userName = userCreds.getUserName();

          // decrypt randomSecret
          final byte[] secretNumberBytes = req.getEncryptedRandomSecret().toByteArray();
          final byte[] secretNumberBytesDecrypted = Decrypt(userKey, secretNumberBytes, err);

          if (secretNumberBytesDecrypted.length != Long.SIZE / 8) {
            final String badSecretError = "Bad random secret";
            LOG.error(badSecretError);
            routingContext.response().putHeader(
                MaprAuthenticator.WWW_ERR_AUTHENTICATE, badSecretError);
            routingContext.fail(401,
                new KsqlApiException("Unauthorized", ERROR_CODE_UNAUTHORIZED));
            return null;
          }
          // CHECKSTYLE_RULES.OFF: BooleanExpressionComplexity
          long returnLong = (((long)secretNumberBytesDecrypted[0] << 56)
              + ((long)(secretNumberBytesDecrypted[1] & 255) << 48)
              + ((long)(secretNumberBytesDecrypted[2] & 255) << 40)
              + ((long)(secretNumberBytesDecrypted[3] & 255) << 32)
              + ((long)(secretNumberBytesDecrypted[4] & 255) << 24)
              + ((secretNumberBytesDecrypted[5] & 255) << 16)
              + ((secretNumberBytesDecrypted[6] & 255) <<  8)
              + ((secretNumberBytesDecrypted[7] & 255) <<  0));
          // CHECKSTYLE_RULES.ON: BooleanExpressionComplexity

          LOG.trace("Received secret number: " + returnLong);

          //increment the secret number and generate response
          returnLong++;

          final Builder authResp = AuthenticationResp.newBuilder();
          authResp.setChallengeResponse(returnLong);
          authResp.setStatus(0);
          final byte [] resp = authResp.build().toByteArray();
          final byte [] respEncrypted = Security.Encrypt(userKey, resp, err);

          final Base64 base64 = new Base64(0);
          final String authenticate = base64.encodeToString(respEncrypted);

          routingContext.response().putHeader(KerberosAuthenticator.AUTHORIZATION,
              MaprAuthenticator.NEGOTIATE + " " + authenticate);

          LOG.trace("MaprAuthentication is completed on server side");

          /* generate the authentication token for the user */
          return new AuthenticationToken(userName, userName, "maprauth");
        } else {
          final String clientRequestError = "Malformed client request";
          LOG.error(clientRequestError);
          routingContext.response().putHeader(
              MaprAuthenticator.WWW_ERR_AUTHENTICATE, clientRequestError);
          routingContext.fail(401,
              new KsqlApiException("Unauthorized", ERROR_CODE_UNAUTHORIZED));

          return null;
        }

      } catch (Throwable t) {
        final String serverKeyError = "Bad server key";

        LOG.error(serverKeyError, t);

        routingContext.response().putHeader(MaprAuthenticator.WWW_ERR_AUTHENTICATE, serverKeyError);
        routingContext.fail(401,
            new KsqlApiException("Unauthorized", ERROR_CODE_UNAUTHORIZED));

        return null;
      }
    }
  }

  // BasicAuthenticationHandler.postauthenticate() reimplmented
  // CHECKSTYLE_RULES.OFF: JavaNCSS
  public AuthenticationToken basicAuthenticate(final RoutingContext routingContext) {
    AuthenticationToken authToken = null;
    String authorization = routingContext.request().getHeader(KerberosAuthenticator.AUTHORIZATION);
    if (authorization != null && authorization.startsWith("Basic")) {
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Credentials: " + authorization);
        }
        authorization = authorization.substring(authorization.indexOf(' ') + 1);
        authorization = new String(Base64.decodeBase64(authorization), StandardCharsets.UTF_8);
        final int i = authorization.indexOf(':');
        final String username = authorization.substring(0, i);
        final String password = authorization.substring(i + 1);
        final Class<?> klass = Thread.currentThread().getContextClassLoader()
                .loadClass("com.mapr.login.PasswordAuthentication");
        final Method passAuth = klass.getDeclaredMethod("authenticate", String.class, String.class);
        if ((boolean) passAuth.invoke(null, username, password)) {
          authToken = new AuthenticationToken(username, username, "basic");
          routingContext.response().setStatusCode(200);
        } else {
          routingContext.response().setStatusCode(401);
          LOG.error("User Principal is null while trying to authenticate with Basic Auth");
        }
      } catch (Exception e) {
        LOG.warn("AUTH FAILURE: " + e);
      }
    }

    return authToken;
  }

  private AuthenticationToken authenticate(final RoutingContext routingContext) {
    final String authorization = routingContext.request().getHeader("Authorization");
    if (authorization != null) {
      if (authorization.startsWith("Basic")) {
        return basicAuthenticate(routingContext);
      }

      if (authorization.startsWith(MaprAuthenticator.NEGOTIATE)) {
        return maprAuthenticate(routingContext);
      }
    }

    routingContext.fail(401, new KsqlApiException("Unauthorized", ERROR_CODE_UNAUTHORIZED));
    return null;
  }

  // AuthenticationFilter.doFilter() reimplemented
  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  // CHECKSTYLE_RULES.OFF: NPathComplexity
  @Override
  public CompletableFuture<Principal> handleAuth(final RoutingContext routingContext,
                                                 final WorkerExecutor workerExecutor) {
    securityHeaders.forEach((k, v) -> {
      routingContext.response().headers().add((String) k, v.toString());
    });

    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    // CHECKSTYLE_RULES.ON: NPathComplexity
    boolean unauthorizedResponse = true;
    int errCode = 401;
    AuthenticationException authenticationEx = null;

    final boolean isHttps = "https".equalsIgnoreCase(routingContext.request().scheme());

    boolean newToken = false;

    AuthenticationToken token;
    try {
      token = this.getToken(routingContext);
    } catch (AuthenticationException e) {
      LOG.warn("AuthenticationToken ignored: " + e.getMessage());
      authenticationEx = e;
      token = null;
    }

    if (token == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Request [{}] triggering authentication",
            routingContext.request().absoluteURI());
      }

      token = authenticate(routingContext);
      if (token != null && token.getExpires() != 0L && token != AuthenticationToken.ANONYMOUS) {
        token.setExpires(System.currentTimeMillis() + this.validity);
      }

      newToken = true;
    }

    if (token != null) {
      unauthorizedResponse = false;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Request [{}] user [{}] authenticated",
            routingContext.request().absoluteURI(),
            token.getUserName());
      }

      if (newToken && !token.isExpired() && token != AuthenticationToken.ANONYMOUS) {
        final String signedToken = this.signer.sign(token.toString());
        final String authCookie = createAuthCookie(
            signedToken, this.cookieDomain, this.cookiePath, token.getExpires(), isHttps);
        routingContext.response().putHeader("Set-Cookie", authCookie);
      }
    }


    if (unauthorizedResponse) {
      final String authCookie =
          createAuthCookie("", this.cookieDomain, this.cookiePath, 0L, isHttps);
      routingContext.response().putHeader("Set-Cookie", authCookie);
      if (errCode == 401 && routingContext.response().headers().contains("WWW-Authenticate")) {
        errCode = 403;
      }

      if (authenticationEx == null) {
        routingContext.response().putHeader(
            MaprAuthenticator.WWW_ERR_AUTHENTICATE, "Authentication required");
        routingContext.fail(errCode,
            new KsqlApiException("Authentication required", ERROR_CODE_UNAUTHORIZED));
      } else {
        routingContext.response().putHeader(
            MaprAuthenticator.WWW_ERR_AUTHENTICATE, authenticationEx.getMessage());
        routingContext.fail(errCode,
            new KsqlApiException(authenticationEx.getMessage(), ERROR_CODE_UNAUTHORIZED));
      }
    }
    return CompletableFuture.completedFuture(new MapRPrincipal(token.getName(),""));
  }

  // AuthenticationFilter.createAuthCookie() reimplemented
  private String createAuthCookie(final String token,
                                  final String domain,
                                  final String path,
                                  final long expires,
                                  final boolean isSecure) {
    final StringBuilder sb = (new StringBuilder("hadoop.auth")).append("=");
    if (token != null && token.length() > 0) {
      sb.append("\"").append(token).append("\"");
    }

    if (path != null) {
      sb.append("; Path=").append(path);
    }

    if (domain != null) {
      sb.append("; Domain=").append(domain);
    }

    if (expires >= 0L) {
      final Date date = new Date(expires);
      final SimpleDateFormat df = new SimpleDateFormat("EEE, dd-MMM-yyyy HH:mm:ss zzz");
      df.setTimeZone(TimeZone.getTimeZone("GMT"));
      sb.append("; Expires=").append(df.format(date));
    }

    if (isSecure) {
      sb.append("; Secure");
    }
    sb.append("; HttpOnly");

    return sb.toString();
  }

}
