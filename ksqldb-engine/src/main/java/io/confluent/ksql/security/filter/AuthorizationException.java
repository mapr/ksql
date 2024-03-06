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

package io.confluent.ksql.security.filter;

import io.confluent.rest.exceptions.RestException;
import javax.ws.rs.core.Response;

public class AuthorizationException extends RestException {

  public static final int DEFAULT_ERROR_CODE = Response.Status.UNAUTHORIZED.getStatusCode();

  public AuthorizationException(final String message) {
    this(message, null);
  }

  public AuthorizationException(final String message, final Throwable cause) {
    super(message, DEFAULT_ERROR_CODE, DEFAULT_ERROR_CODE, cause);
  }
}
