package io.confluent.ksql.rest.filter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.filter.util.ByteConsumerPool;
import io.confluent.ksql.rest.filter.util.ByteProducerPool;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.util.KsqlConfig;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.easymock.Capture;
import org.easymock.EasyMockSupport;
import org.glassfish.jersey.server.ContainerRequest;
import org.hamcrest.Matcher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.MockPolicy;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static com.google.common.collect.ImmutableList.of;
import static io.confluent.ksql.rest.MockMatchers.*;
import static java.util.concurrent.CompletableFuture.*;
import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

@RunWith(PowerMockRunner.class)
@MockPolicy(UserGroupInformationMockPolicy.class)
@PowerMockRunnerDelegate(JUnitParamsRunner.class)
public class AuthorizationFilterTest extends EasyMockSupport {

  private static final String AUXILIARY_TOPIC = "/apps/ksql/test-service/ksql-commands:ksql-authorization-auxiliary-topic";
  private static final String ALWAYS_ALLOWED_PATH = "info";
  private static final String NONE = "NONE";
  private static final String BASIC_AUTH = "Basic dXNlcjp1c2Vy";
  private static final String COOKIE_AUTH = "hadoop.auth=smt&u=user";
  private static final String IMPERSONATED_USER = "user";
  private static final String ADMIN_USER = System.getProperty("user.name");
  private ByteConsumerPool consumerPool;
  private ByteProducerPool producerPool;
  private AuthorizationFilter authorizationFilter;
  private KsqlRestConfig restConfig;

  @Before
  public void setUp() {
    consumerPool = mock(ByteConsumerPool.class);
    producerPool = mock(ByteProducerPool.class);
    expectUserSendsRecordToAuxiliaryTopicSucceeds(ADMIN_USER);
    replayAll();
    restConfig = new KsqlRestConfig(ImmutableMap.of(
            KsqlRestConfig.LISTENERS_CONFIG, "meaningless",
            KsqlConfig.KSQL_SERVICE_ID_CONFIG, "test-service"
    ));
    authorizationFilter = new AuthorizationFilter(restConfig, consumerPool, producerPool);
    resetAll();
  }

  @Test
  public void dummyRecordIsSent() {
    expectUserSendsRecordToAuxiliaryTopicSucceeds(ADMIN_USER);
    replayAll();

    new AuthorizationFilter(restConfig, consumerPool, producerPool);

    verifyAll();
  }

  @Test
  public void unauthenticatedRequestIsForbidden() throws IOException {
    final ContainerRequest request = mock(ContainerRequest.class);
    expectGatherAuthData(request, null, null);
    final Capture<Response> capturedResponse = Capture.newInstance();
    expectAbortWith(request, capture(capturedResponse));
    replayAll();

    authorizationFilter.filter(request);

    verifyAll();
    assertThat(capturedResponse.getValue(), allOf(
        hasStatus(Response.Status.FORBIDDEN),
        hasErrorMessageThat(containsString("user"))
    ));
  }

  private Matcher<Response> hasStatus(Response.Status status) {
    return hasProperty("status", equalTo(status.getStatusCode()));
  }

  private Matcher<Response> hasErrorMessageThat(Matcher<String> matcher) {
    return hasProperty("entity", allOf(
        instanceOf(KsqlErrorMessage.class),
        hasProperty("message", matcher)
    ));
  }

  @Test
  @Parameters(value = {
          BASIC_AUTH + " | " + NONE,
          NONE + " | " + COOKIE_AUTH
  })
  public void authenticatedRequestSucceeds(String authorizationHeader, String cookie) throws IOException {
    final ContainerRequest request = mock(ContainerRequest.class);
    expectGatherAuthData(request, authorizationHeader.equals(NONE) ? null : authorizationHeader, cookie.equals(NONE) ? null : of(cookie));
    expect(request.getPath(true)).andReturn(ALWAYS_ALLOWED_PATH);
    replayAll();

    authorizationFilter.filter(request);

    verifyAll();
  }

  @Test
  public void unsupportedRequestIsNotImplemented() throws IOException {
    final ContainerRequest request = mock(ContainerRequest.class);
    expectGatherAuthData(request, BASIC_AUTH, of(COOKIE_AUTH));
    expect(request.getPath(true)).andReturn("unsupported-path");
    final Capture<Response> capturedResponse = Capture.newInstance();
    expectAbortWith(request, capture(capturedResponse));
    replayAll();

    authorizationFilter.filter(request);

    verifyAll();
    assertThat(capturedResponse.getValue(), allOf(
        hasStatus(Response.Status.NOT_IMPLEMENTED),
        hasErrorMessageThat(containsString("supported"))
    ));
  }

  @Test
  public void infoRequestSucceedsWithoutReadWritePermissions() throws IOException {
    final ContainerRequest request = mock(ContainerRequest.class);
    expectGatherAuthData(request, BASIC_AUTH, of(COOKIE_AUTH));
    expect(request.getPath(true)).andReturn("info");
    replayAll();

    authorizationFilter.filter(request);

    verifyAll();
  }

  @Test
  @Parameters(value = {
          "CREATE", "DROP", "RUN", "TERMINATE", "INSERT"
  })
  public void writeCommandIsForbiddenWithoutWritePermission(final String cmd) throws IOException {
    final ContainerRequest request = mock(ContainerRequest.class);
    expectGatherAuthData(request, BASIC_AUTH, of(COOKIE_AUTH));
    expect(request.getPath(true)).andReturn("ksql");
    expectReadEntityAndWriteItBack(request, "{\"ksql\": \"" + cmd + "\"}");
    final Capture<Response> captured = Capture.newInstance();
    expectAbortWith(request, capture(captured));
    expectUserSendsRecordToAuxiliaryTopicFails(IMPERSONATED_USER);
    replayAll();

    authorizationFilter.filter(request);

    verifyAll();
    assertThat(captured.getValue(), allOf(
        hasStatus(Response.Status.FORBIDDEN),
        hasErrorMessageThat(containsString("permission"))
    ));
  }

  @Test
  @Parameters(value = {
          "CREATE", "DROP", "RUN", "TERMINATE", "INSERT"
  })
  public void writeCommandSucceedsWithWritePermission(final String cmd) throws IOException {
    final ContainerRequest request = mock(ContainerRequest.class);
    expectGatherAuthData(request, BASIC_AUTH, of(COOKIE_AUTH));
    expect(request.getPath(true)).andReturn("ksql");
    expectReadEntityAndWriteItBack(request, "{\"ksql\": \"" + cmd + "\"}");
    expectUserSendsRecordToAuxiliaryTopicSucceeds(IMPERSONATED_USER);
    replayAll();

    authorizationFilter.filter(request);

    verifyAll();
  }

  @Test
  @Parameters(value = {
          "ksql|{\"ksql\": \"DESCRIBE\"}",
          "ksql|{\"ksql\": \"EXPLAIN\"}",
          "ksql|{\"ksql\": \"SHOW TABLES\"}",
          "ksql|{\"ksql\": \"SHOW STREAMS\"}",
          "ksql|{\"ksql\": \"anything else\"}",
          "query|" + NONE,
          "status|" + NONE,
          "|" + NONE
  })
  public void readCommandIsForbiddenWithoutReadPermission(String path, String entity) throws IOException {
    final ContainerRequest request = mock(ContainerRequest.class);
    expectGatherAuthData(request, BASIC_AUTH, of(COOKIE_AUTH));
    expect(request.getPath(true)).andReturn(path);
    if (!entity.equals(NONE)) {
      expectReadEntityAndWriteItBack(request, entity);
    }
    final Capture<Response> captured = Capture.newInstance();
    expectAbortWith(request, capture(captured));
    expectUserPollsRecordsToAuxiliaryTopicFails(IMPERSONATED_USER);
    replayAll();

    authorizationFilter.filter(request);

    verifyAll();
    assertThat(captured.getValue(), allOf(
        hasStatus(Response.Status.FORBIDDEN),
        hasErrorMessageThat(containsString("permission"))
    ));
  }

  @Test
  @Parameters(value = {
          "ksql|{\"ksql\": \"DESCRIBE\"}",
          "ksql|{\"ksql\": \"EXPLAIN\"}",
          "ksql|{\"ksql\": \"SHOW TABLES\"}",
          "ksql|{\"ksql\": \"SHOW STREAMS\"}",
          "ksql|{\"ksql\": \"anything else\"}",
          "query|" + NONE,
          "status|" + NONE,
          "|" + NONE
  })
  public void readCommandSucceedsWithReadPermission(String path, String entity) throws IOException {
    final ContainerRequest request = mock(ContainerRequest.class);
    expectGatherAuthData(request, BASIC_AUTH, of(COOKIE_AUTH));
    expect(request.getPath(true)).andReturn(path);
    if (!entity.equals(NONE)) {
      expectReadEntityAndWriteItBack(request, entity);
    }
    expectUserPollsRecordsToAuxiliaryTopicSucceeds(IMPERSONATED_USER);
    replayAll();

    authorizationFilter.filter(request);

    verifyAll();
  }

  private void expectGatherAuthData(ContainerRequest request, String header, List<String> cookie) {
    expect(request.getHeaderString(HttpHeaders.AUTHORIZATION)).andReturn(header);
    expect(request.getRequestHeader(HttpHeaders.COOKIE)).andReturn(cookie);
  }

  private void expectReadEntityAndWriteItBack(ContainerRequest request, String text) {
    expect(request.getEntityStream()).andReturn(IOUtils.toInputStream(text, Charset.defaultCharset()));
    request.setEntityStream(inputStreamOf(text));
    expectLastCall();
  }

  private void expectAbortWith(ContainerRequest request, Response response) {
    request.abortWith(response);
    expectLastCall().once();
  }

  private void expectUserSendsRecordToAuxiliaryTopicSucceeds(String user) {
    expect(producerPool.send(new ProducerRecord<>(AUXILIARY_TOPIC, anyObject(byte[].class)))).andAnswer(() -> {
      assertUserIs(user);
      final TopicPartition partition = new TopicPartition(AUXILIARY_TOPIC, 1);
      return completedFuture(new RecordMetadata(partition, 0, 0));
    });
  }

  private void expectUserSendsRecordToAuxiliaryTopicFails(String user) {
    expect(producerPool.send(new ProducerRecord<>(AUXILIARY_TOPIC, anyObject(byte[].class)))).andAnswer(() -> {
      assertUserIs(user);
      return supplyAsync(() -> {
        throw new RuntimeException();
      });
    });
  }

  private void expectUserPollsRecordsToAuxiliaryTopicSucceeds(String user) {
    expect(consumerPool.poll()).andAnswer(() -> {
      assertUserIs(user);
      return new ConsumerRecords<>(ImmutableMap.of(
          new TopicPartition("topic", 0),
          ImmutableList.of(new ConsumerRecord<>(
              "topic", 0, 0,
              "key".getBytes(), "value".getBytes()))
      ));
    });
  }

  private void expectUserPollsRecordsToAuxiliaryTopicFails(String user) {
    expect(consumerPool.poll()).andAnswer(() -> {
      assertUserIs(user);
      throw new RuntimeException();
    });
  }

  private void assertUserIs(String user) throws IOException {
    final String actualUser = UserGroupInformation.getCurrentUser().getUserName();
    final String msg = String.format("Expected user is %s while actual is %s", user, actualUser);
    Assert.assertEquals(msg, user, actualUser);
  }
}