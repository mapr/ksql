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

import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import java.security.Principal;
import java.util.Optional;

public final class DefaultApiSecurityContext implements ApiSecurityContext {

  private final Optional<Principal> principal;
  private final Optional<String> authToken;
  // getting null here is expected by external lib
  private final Optional<String> cookie;

  public static DefaultApiSecurityContext create(final RoutingContext routingContext) {
    final User user = routingContext.user();
    if (user != null && !(user instanceof ApiUser)) {
      throw new IllegalStateException("Not an ApiUser: " + user);
    }
    final ApiUser apiUser = (ApiUser) user;
    final String authToken = routingContext.request().getHeader("Authorization");
    final String cookie = routingContext.request().getHeader("Cookie");
    return new DefaultApiSecurityContext(apiUser != null ? apiUser.getPrincipal() : null,
        authToken, cookie);
  }

  private DefaultApiSecurityContext(final Principal principal, final String authToken,
                                    final String cookie) {
    this.principal = Optional.ofNullable(principal);
    this.authToken = Optional.ofNullable(authToken);
    this.cookie = Optional.ofNullable(cookie);
  }

  @Override
  public Optional<Principal> getPrincipal() {
    return principal;
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
