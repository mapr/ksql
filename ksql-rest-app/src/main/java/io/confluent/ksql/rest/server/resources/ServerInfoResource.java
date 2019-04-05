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

package io.confluent.ksql.rest.server.resources;

import io.confluent.ksql.rest.entity.Versions;
import io.confluent.rest.impersonation.ImpersonationUtils;

import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/info")
@Produces({Versions.KSQL_V1_JSON, MediaType.APPLICATION_JSON})
public class ServerInfoResource {

  private final io.confluent.ksql.rest.entity.ServerInfo serverInfo;

  public ServerInfoResource(final io.confluent.ksql.rest.entity.ServerInfo serverInfo) {
    this.serverInfo = serverInfo;
  }

  @GET
  public Response get(@HeaderParam(HttpHeaders.AUTHORIZATION) final String auth,
                      @HeaderParam(HttpHeaders.COOKIE) final String cookie) {
    return ImpersonationUtils.runAsUserIfImpersonationEnabled(()
        -> get(), auth, cookie);
  }

  private Response get() {
    return Response.ok(serverInfo).build();
  }
}
