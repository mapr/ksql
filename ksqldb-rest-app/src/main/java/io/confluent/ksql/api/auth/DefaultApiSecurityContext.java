/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api.auth;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.api.server.Server;
import io.confluent.ksql.security.KsqlPrincipal;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

public final class DefaultApiSecurityContext implements ApiSecurityContext {
  private final Optional<KsqlPrincipal> principal;
  private final Optional<String> authToken;
  private final List<Entry<String, String>> requestHeaders;
  // getting null here is expected by external lib
  private final Optional<String> cookie;

  public static DefaultApiSecurityContext create(final RoutingContext routingContext,
      final Server server) {
    final User user = routingContext.user();
    if (user != null && !(user instanceof ApiUser)) {
      throw new IllegalStateException("Not an ApiUser: " + user);
    }
    final ApiUser apiUser = (ApiUser) user;

    String authToken = routingContext.request().getHeader("Authorization");
    final String cookie = routingContext.request().getHeader("Cookie");
    if (server.getAuthenticationPlugin().isPresent()) {
      authToken = server.getAuthenticationPlugin().get().getAuthHeader(routingContext);
    }

    final List<Entry<String, String>> requestHeaders = routingContext.request().headers().entries();
    final String ipAddress = routingContext.request().remoteAddress().host();
    final int port = routingContext.request().remoteAddress().port();

    return new DefaultApiSecurityContext(
        apiUser != null
            ? apiUser.getPrincipal().withIpAddressAndPort(ipAddress == null ? "" : ipAddress, port)
            : null,
        authToken,
        cookie,
        requestHeaders);
  }

  private DefaultApiSecurityContext(
      final KsqlPrincipal principal,
      final String authToken,
      final String cookie,
      final List<Entry<String, String>> requestHeaders) {
    this.principal = Optional.ofNullable(principal);
    this.authToken = Optional.ofNullable(authToken);
    this.requestHeaders = requestHeaders;
    this.cookie = Optional.ofNullable(cookie);
  }

  @Override
  public Optional<KsqlPrincipal> getPrincipal() {
    return principal;
  }

  @Override
  public Optional<String> getAuthHeader() {
    return authToken;
  }

  @Override
  public List<Entry<String, String>> getRequestHeaders() {
    return ImmutableList.copyOf(requestHeaders);
  }

  @Override
  public Optional<String> getAuthToken() {
    return authToken;
  }

  @Override
  public Optional<String> getCookie() {
    return cookie;
  }
}
