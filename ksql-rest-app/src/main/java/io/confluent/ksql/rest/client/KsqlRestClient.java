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

package io.confluent.ksql.rest.client;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.confluent.ksql.rest.client.exception.AuthorizationException;
import io.confluent.ksql.rest.client.exception.KsqlRestClientException;
import io.confluent.ksql.rest.client.properties.LocalProperties;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.resources.Errors;
import io.confluent.ksql.rest.util.JsonMapper;
import io.confluent.ksql.util.Pair;
import io.confluent.rest.RestConfig;
import io.confluent.rest.validation.JacksonMessageBodyProvider;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Scanner;
import java.util.function.Function;
import javax.naming.AuthenticationException;
import javax.net.ssl.SSLContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.compress.utils.IOUtils;

import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.UNAUTHORIZED;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class KsqlRestClient implements Closeable {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final KsqlErrorMessage UNAUTHORIZED_ERROR_MESSAGE = new KsqlErrorMessage(
      Errors.ERROR_CODE_UNAUTHORIZED,
      new AuthenticationException(
          "Could not authenticate successfully with the supplied credentials or ticket.")
  );

  private static final KsqlErrorMessage FORBIDDEN_ERROR_MESSAGE = new KsqlErrorMessage(
      Errors.ERROR_CODE_FORBIDDEN,
      new AuthenticationException("You are forbidden from using this cluster.")
  );

  private final Client client;

  private URI serverAddress;

  private final LocalProperties localProperties;

  private final String authMethod;
  private String authHeader;

  public KsqlRestClient(final String serverAddress) {
    this(serverAddress, Collections.emptyMap(), Optional.ofNullable(null));
  }

  public KsqlRestClient(final String serverAddress,
                        final Properties properties,
                        final Optional<String> authMethod) {
    this(serverAddress, propertiesToMap(properties), authMethod);
  }

  public KsqlRestClient(final String serverAddress,
                        final Map<String, Object> localProperties,
                        final Optional<String> authMethod) {
    this(buildClient(Optional.ofNullable(localProperties.get(RestConfig.SSL_TRUSTALLCERTS_CONFIG))
              .map(x -> Boolean.valueOf(x.toString()))
              .orElse(false), initializeSslContext(serverAddress, localProperties)),
            serverAddress, localProperties, authMethod);
  }

  private static SSLContext initializeSslContext(final String serverAddress,
                                                 final Map<String, Object> localProperties) {
    if (serverAddress.trim().startsWith("https://")) {
      return new SslContextFactory(localProperties).sslContext();
    }
    return null;
  }

  // Visible for testing
  KsqlRestClient(final Client client,
                 final String serverAddress,
                 final Map<String, Object> localProperties,
                 final Optional<String> authMethod) {
    this.client = Objects.requireNonNull(client, "client");
    this.serverAddress = parseServerAddress(serverAddress);
    this.localProperties = new LocalProperties(localProperties);
    if (authMethod.isPresent()) {
      this.authMethod = authMethod.get();
      setupAuthenticationCredentials(false);
      authenticate();
    } else {
      this.authMethod = "None";
    }
  }

  private void setupAuthenticationCredentials(boolean sessionExpired) {
    if (authMethod.equalsIgnoreCase("basic")) {
      final Pair<String, String> credentials = AuthenticationUtils
              .readUsernameAndPassword(sessionExpired);
      this.setBasicAuthHeader(Objects.requireNonNull(credentials.left),
              Objects.requireNonNull(credentials.right));
    }
    if (authMethod.equalsIgnoreCase("maprsasl")) {
      final String readChallangeString = AuthenticationUtils.readChallengeString();
      this.setMapRSaslAuthHeader(readChallangeString);
    }
  }

  private void authenticate() {
    final RestResponse<ServerInfo> response = getServerInfo();
    if (response.isErroneous()) {
      final KsqlErrorMessage ksqlError = response.getErrorMessage();
      final int errorCode = Errors.toStatusCode(ksqlError.getErrorCode());
      if (errorCode == FORBIDDEN.getStatusCode()
              ||
              errorCode == UNAUTHORIZED.getStatusCode()) {
        final String errorMsg = String.format("%s%n%s%n%s%n",
                "**************** ERROR ********************",
                "Could not authenticate successfully with the supplied credentials or ticket.",
                "*******************************************");

        throw new AuthorizationException(errorMsg);
      }
    }
  }

  public URI getServerAddress() {
    return serverAddress;
  }

  public void setServerAddress(final String serverAddress) {
    this.serverAddress = parseServerAddress(serverAddress);
  }

  public RestResponse<ServerInfo> makeRootRequest() {
    return getServerInfo();
  }

  public RestResponse<ServerInfo> getServerInfo() {
    return getRequest("/info", ServerInfo.class);
  }

  public RestResponse<KsqlEntityList> makeKsqlRequest(final String ksql) {
    final KsqlRequest jsonRequest = new KsqlRequest(ksql, localProperties.toMap());
    return postRequest("ksql", jsonRequest, true, r -> r.readEntity(KsqlEntityList.class));
  }

  public RestResponse<CommandStatuses> makeStatusRequest() {
    return getRequest("status", CommandStatuses.class);
  }

  public RestResponse<CommandStatus> makeStatusRequest(final String commandId) {
    return getRequest(String.format("status/%s", commandId), CommandStatus.class);
  }

  public RestResponse<QueryStream> makeQueryRequest(final String ksql) {
    final KsqlRequest jsonRequest = new KsqlRequest(ksql, localProperties.toMap());
    return postRequest("query", jsonRequest, false, QueryStream::new);
  }

  public RestResponse<InputStream> makePrintTopicRequest(final String ksql) {
    final KsqlRequest jsonRequest = new KsqlRequest(ksql, localProperties.toMap());
    return postRequest("query", jsonRequest, false, r -> (InputStream)r.getEntity());
  }

  @Override
  public void close() {
    client.close();
  }

  private <T> RestResponse<T> getRequest(final String path, final Class<T> type) {

    final javax.ws.rs.client.Invocation.Builder requestBuilder =
            client.target(serverAddress).path(path)
                    .request(MediaType.APPLICATION_JSON_TYPE);
    setAuthHeaderOrCookieIfNeeded(requestBuilder);

    Response response = null;

    try {
      response = requestBuilder.get();
      final boolean retry = reAuthenticateIfNeeded(response);
      if (retry) {
        response.close();
        final javax.ws.rs.client.Invocation.Builder newRequestBuilder =
                client.target(serverAddress).path(path)
                .request(MediaType.APPLICATION_JSON_TYPE);
        setAuthHeaderOrCookieIfNeeded(newRequestBuilder);
        response = newRequestBuilder.get();
      }
      extractAuthCookieFromResponse(response);

      return response.getStatus() == Response.Status.OK.getStatusCode()
          ? RestResponse.successful(response.readEntity(type))
          : createErrorResponse(path, response);

    } catch (final AuthorizationException e) {
      throw e;
    } catch (final Exception e) {
      throw new KsqlRestClientException("Error issuing GET to KSQL server. path:" + path, e);
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }

  private <T> RestResponse<T> postRequest(
      final String path,
      final Object jsonEntity,
      final boolean closeResponse,
      final Function<Response, T> mapper) {

    final javax.ws.rs.client.Invocation.Builder requestBuilder =  client.target(serverAddress)
            .path(path)
            .request(MediaType.APPLICATION_JSON_TYPE);
    setAuthHeaderOrCookieIfNeeded(requestBuilder);

    Response response = null;

    try {
      response = requestBuilder.post(Entity.json(jsonEntity));
      final boolean retry = reAuthenticateIfNeeded(response);
      if (retry) {
        response.close();
        final javax.ws.rs.client.Invocation.Builder newRequestBuilder =
                client.target(serverAddress).path(path)
                .request(MediaType.APPLICATION_JSON_TYPE);
        setAuthHeaderOrCookieIfNeeded(newRequestBuilder);
        response = newRequestBuilder.post(Entity.json(jsonEntity));
      }
      extractAuthCookieFromResponse(response);

      return response.getStatus() == Response.Status.OK.getStatusCode()
          ? RestResponse.successful(mapper.apply(response))
          : createErrorResponse(path, response);

    } catch (final AuthorizationException e) {
      throw e;
    } catch (final Exception e) {
      throw new KsqlRestClientException("Error issuing POST to KSQL server. path:" + path, e);
    } finally {
      if (response != null && closeResponse) {
        response.close();
      }
    }
  }

  private static <T> RestResponse<T> createErrorResponse(
      final String path,
      final Response response) {

    final KsqlErrorMessage errorMessage = response.readEntity(KsqlErrorMessage.class);
    if (errorMessage != null) {
      return RestResponse.erroneous(errorMessage);
    }

    if (response.getStatus() == Status.NOT_FOUND.getStatusCode()) {
      return RestResponse.erroneous(404, "Path not found. Path='" + path + "'. "
          + "Check your ksql http url to make sure you are connecting to a ksql server.");
    }

    if (response.getStatus() == Status.UNAUTHORIZED.getStatusCode()) {
      return RestResponse.erroneous(UNAUTHORIZED_ERROR_MESSAGE);
    }

    if (response.getStatus() == Status.FORBIDDEN.getStatusCode()) {
      return RestResponse.erroneous(FORBIDDEN_ERROR_MESSAGE);
    }

    return RestResponse.erroneous(
        Errors.toErrorCode(response.getStatus()), "The server returned an unexpected error.");
  }

  private void setMapRSaslAuthHeader(final String challangeString) {
    authHeader = String.format("MAPR-Negotiate %s", challangeString);
  }

  private void setBasicAuthHeader(final String user,
                                  final String password) {
    final byte[] userPass;
    try {
      userPass = String.format("%s:%s",user, password).getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    final String encoding = Base64.getEncoder().encodeToString(userPass);
    authHeader = String.format("Basic %s", encoding);
  }

  private void extractAuthCookieFromResponse(Response response) {
    if (authHeader == null
         ||
        response.getStatus() != Response.Status.OK.getStatusCode()) {
      return;
    }

    final Optional<String> hadoopAuth = Optional.ofNullable(response.getHeaderString("Set-Cookie"));
    hadoopAuth.ifPresent((value) -> {
      if (value.startsWith("hadoop.auth")) {
        authHeader = value;
      }
    });
  }

  private boolean reAuthenticateIfNeeded(Response response) {
    if (authHeader == null) {
      return false;
    }

    if (response.getStatus() == Status.UNAUTHORIZED.getStatusCode()
        &&
        authHeader.startsWith("hadoop.auth")) {

      setupAuthenticationCredentials(true);
      authenticate();

      return true;
    }

    return false;
  }

  private void setAuthHeaderOrCookieIfNeeded(javax.ws.rs.client.Invocation.Builder requestBuilder) {
    if (authHeader == null) {
      return;
    }

    if (authHeader.startsWith("hadoop.auth")) {
      requestBuilder.header(HttpHeaders.COOKIE, authHeader);
    } else {
      requestBuilder.header(HttpHeaders.AUTHORIZATION, authHeader);
    }
  }

  public static final class QueryStream implements Closeable, Iterator<StreamedRow> {

    private final Response response;
    private final ObjectMapper objectMapper;
    private final Scanner responseScanner;
    private final InputStreamReader isr;

    private StreamedRow bufferedRow;
    private volatile boolean closed = false;

    private QueryStream(final Response response) {
      this.response = response;

      this.objectMapper = new ObjectMapper();
      this.isr = new InputStreamReader(
          (InputStream) response.getEntity(),
          StandardCharsets.UTF_8
      );
      this.responseScanner = new Scanner((buf) -> {
        while (true) {
          try {
            return isr.read(buf);
          } catch (final SocketTimeoutException e) {
            // Read timeout:
            if (closed) {
              return -1;
            }
          } catch (final IOException e) {
            // Can occur if isr closed:
            if (closed) {
              return -1;
            }

            throw e;
          }
        }
      });

      this.bufferedRow = null;
    }

    @Override
    public boolean hasNext() {
      if (closed) {
        throw closedIllegalStateException("hasNext()");
      }

      if (bufferedRow != null) {
        return true;
      }

      while (responseScanner.hasNextLine()) {
        final String responseLine = responseScanner.nextLine().trim();
        if (!responseLine.isEmpty()) {
          try {
            bufferedRow = objectMapper.readValue(responseLine, StreamedRow.class);
          } catch (final IOException exception) {
            // TODO: Should the exception be handled somehow else?
            // Swallowing it silently seems like a bad idea...
            throw new RuntimeException(exception);
          }
          return true;
        }
      }

      return false;
    }

    @Override
    public StreamedRow next() {
      if (closed) {
        throw closedIllegalStateException("next()");
      }

      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      final StreamedRow result = bufferedRow;
      bufferedRow = null;
      return result;
    }

    @Override
    public void close() {
      if (closed) {
        throw closedIllegalStateException("close()");
      }

      synchronized (this) {
        closed = true;
        this.notifyAll();
      }
      responseScanner.close();
      response.close();
      IOUtils.closeQuietly(isr);
    }

    private IllegalStateException closedIllegalStateException(final String methodName) {
      return new IllegalStateException("Cannot call " + methodName + " when QueryStream is closed");
    }
  }

  public Object setProperty(final String property, final Object value) {
    return localProperties.set(property, value);
  }

  public Object unsetProperty(final String property) {
    return localProperties.unset(property);
  }

  private static Map<String, Object> propertiesToMap(final Properties properties) {
    final Map<String, Object> propertiesMap = new HashMap<>();
    properties.stringPropertyNames().forEach(
        prop -> propertiesMap.put(prop, properties.getProperty(prop)));

    return propertiesMap;
  }

  private static URI parseServerAddress(final String serverAddress) {
    Objects.requireNonNull(serverAddress, "serverAddress");
    try {
      return new URL(serverAddress).toURI();
    } catch (final Exception e) {
      throw new KsqlRestClientException(
          "The supplied serverAddress is invalid: " + serverAddress, e);
    }
  }

  private static Client buildClient(final boolean insecure, final SSLContext sslContext) {
    final ObjectMapper objectMapper = JsonMapper.INSTANCE.mapper;
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
    objectMapper.registerModule(new Jdk8Module());
    final JacksonMessageBodyProvider jsonProvider = new JacksonMessageBodyProvider(objectMapper);
    final ClientBuilder cb = ClientBuilder.newBuilder();
    if (sslContext == null) {
      return cb.register(jsonProvider).build();
    }
    if (insecure) {
      cb.hostnameVerifier((x, y) -> true);
    }
    return cb.sslContext(sslContext).register(jsonProvider).build();
  }
}
