package io.confluent.ksql.security.filter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.DropStatement;
import io.confluent.ksql.parser.tree.ListTables;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.security.KsqlPrincipal;
import io.confluent.ksql.security.KsqlSecurityContext;
import io.confluent.ksql.security.filter.util.ByteConsumerPool;
import io.confluent.ksql.security.filter.util.ByteProducerPool;
import io.confluent.ksql.test.util.UserGroupInformationMockPolicy;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.MockPolicy;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Optional;

import static java.util.concurrent.CompletableFuture.*;
import static org.easymock.EasyMock.*;

@RunWith(PowerMockRunner.class)
@MockPolicy(UserGroupInformationMockPolicy.class)
@PowerMockIgnore("javax.security.*")
public class AuthorizationFilterTest extends EasyMockSupport {

  private static final String AUXILIARY_TOPIC = "/apps/ksql/test-service/ksql-commands:ksql-authorization-auxiliary-topic";
  private static final String IMPERSONATED_USER = "user";
  private static final String ADMIN_USER = System.getProperty("user.name");
  private ByteConsumerPool consumerPool;
  private ByteProducerPool producerPool;
  private AuthorizationFilter authorizationFilter;

  @Before
  public void setUp() {
    consumerPool = mock(ByteConsumerPool.class);
    producerPool = mock(ByteProducerPool.class);
    authorizationFilter = new AuthorizationFilter(consumerPool, producerPool, AUXILIARY_TOPIC);
  }

  @Test
  public void initializesDummyRecord() {
    expectUserSendsRecordToAuxiliaryTopicSucceeds(ADMIN_USER);
    replayAll();

    authorizationFilter.initialize();

    verifyAll();
  }

  @Test(expected = KafkaException.class)
  public void rethrowsInitializationExceptionsAsUnchecked() {
    expect(producerPool.send(new ProducerRecord<>(AUXILIARY_TOPIC, anyObject(byte[].class))))
        .andThrow(new RuntimeException());
    replayAll();

    authorizationFilter.initialize();

    verifyAll();
  }

  @Test
  public void authenticatedRequestSucceeds() {
    final KsqlSecurityContext securityContext = createKsqlSecurityContext();
    final MetaStore metaStore = mock(MetaStore.class);
    final Statement statement = mock(Statement.class);
    ConsumerRecords<byte[], byte[]> pollResult = mock(ConsumerRecords.class);
    expect(consumerPool.poll(AUXILIARY_TOPIC)).andReturn(pollResult);
    expect(pollResult.count()).andReturn(1);
    replayAll();

    authorizationFilter.checkAuthorization(securityContext, metaStore, statement);

    verifyAll();
  }

  @Test(expected = KsqlTopicAuthorizationException.class)
  public void writeCommandIsForbiddenWithoutWritePermission() {
    final KsqlSecurityContext securityContext = createKsqlSecurityContext();
    final MetaStore metaStore = mock(MetaStore.class);
    final Statement statement = mock(DropStatement.class);
    expectUserSendsRecordToAuxiliaryTopicFails(IMPERSONATED_USER);
    replayAll();

    authorizationFilter.checkAuthorization(securityContext, metaStore, statement);

    verifyAll();
  }

  @Test
  public void writeCommandSucceedsWithWritePermission() {
    final KsqlSecurityContext securityContext = createKsqlSecurityContext();
    final MetaStore metaStore = mock(MetaStore.class);
    final Statement statement = mock(DropStatement.class);
    expectUserSendsRecordToAuxiliaryTopicSucceeds(IMPERSONATED_USER);
    replayAll();

    authorizationFilter.checkAuthorization(securityContext, metaStore, statement);

    verifyAll();
  }

  @Test(expected = KsqlTopicAuthorizationException.class)
  public void readCommandIsForbiddenWithoutReadPermission() {
    final KsqlSecurityContext securityContext = createKsqlSecurityContext();
    final MetaStore metaStore = mock(MetaStore.class);
    final Statement statement = mock(ListTables.class);
    expectUserPollsRecordsToAuxiliaryTopicFails(IMPERSONATED_USER);
    replayAll();

    authorizationFilter.checkAuthorization(securityContext, metaStore, statement);

    verifyAll();
  }

  @Test(expected = KsqlTopicAuthorizationException.class)
  public void readCommandIsForbiddenWhenPollsEmptyRecords() {
    final KsqlSecurityContext securityContext = createKsqlSecurityContext();
    final MetaStore metaStore = mock(MetaStore.class);
    final Statement statement = mock(Statement.class);
    expect(consumerPool.poll(AUXILIARY_TOPIC)).andAnswer(() -> new ConsumerRecords<>(ImmutableMap.of()));
    replayAll();

    authorizationFilter.checkAuthorization(securityContext, metaStore, statement);

    verifyAll();
  }

  @Test
  public void readCommandSucceedsWithReadPermission() {
    final KsqlSecurityContext securityContext = createKsqlSecurityContext();
    final MetaStore metaStore = mock(MetaStore.class);
    final Statement statement = mock(ListTables.class);
    expectUserPollsRecordsToAuxiliaryTopicSucceeds(IMPERSONATED_USER);
    replayAll();

    authorizationFilter.checkAuthorization(securityContext, metaStore, statement);

    verifyAll();
  }

  private KsqlSecurityContext createKsqlSecurityContext() {
    final KsqlSecurityContext context = mock(KsqlSecurityContext.class);
    final KsqlPrincipal principal = mock(KsqlPrincipal.class);
    expect(context.getUserPrincipal()).andStubReturn(Optional.of(principal));
    expect(principal.getName()).andReturn(IMPERSONATED_USER);

    return context;
  }

  private void expectUserSendsRecordToAuxiliaryTopicSucceeds(String user) {
    expect(producerPool.send(new ProducerRecord<>(AUXILIARY_TOPIC, anyObject(byte[].class)))).andAnswer(() -> {
      assertUserIs(user);
      final TopicPartition partition = new TopicPartition(AUXILIARY_TOPIC, 1);
      return completedFuture(new RecordMetadata(partition, 0, 0, 0, 0, 0));
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
    expect(consumerPool.poll(AUXILIARY_TOPIC)).andAnswer(() -> {
      assertUserIs(user);
      return new ConsumerRecords<>(ImmutableMap.of(
          new TopicPartition("topic", 0),
          ImmutableList.of(new ConsumerRecord<>(
              "topic", 0, 0,
              "key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8)))
      ));
    });
  }

  private void expectUserPollsRecordsToAuxiliaryTopicFails(String user) {
    expect(consumerPool.poll(AUXILIARY_TOPIC)).andAnswer(() -> {
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