package io.confluent.ksql.test.util;

import org.apache.hadoop.security.UserGroupInformation;
import org.easymock.Capture;
import org.easymock.IAnswer;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.support.SafeExceptionRethrower;
import org.powermock.core.spi.PowerMockPolicy;
import org.powermock.mockpolicies.MockPolicyClassLoadingSettings;
import org.powermock.mockpolicies.MockPolicyInterceptionSettings;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import static org.easymock.EasyMock.*;

@SuppressWarnings("ConstantConditions")
public class UserGroupInformationMockPolicy implements PowerMockPolicy {

  private static final String USER_NAME = System.getProperty("user.name");
  private static AtomicReference<UserGroupInformation> currentUser = new AtomicReference<>();

  @Override
  public void applyClassLoadingPolicy(MockPolicyClassLoadingSettings mockPolicyClassLoadingSettings) {
    mockPolicyClassLoadingSettings.addFullyQualifiedNamesOfClassesToLoadByMockClassloader(
        UserGroupInformation.class.getName()
    );
  }

  @Override
  public void applyInterceptionPolicy(MockPolicyInterceptionSettings mockPolicyInterceptionSettings) {
    PowerMock.mockStatic(UserGroupInformation.class);
    try {
      stubRethrowing();
    } catch (Exception e) {
      SafeExceptionRethrower.safeRethrow(e);
    }
    PowerMock.replay(UserGroupInformation.class);
  }

  private void stubRethrowing() throws Exception {
    UserGroupInformation loginUser = stubUser(USER_NAME, null);
    expect(UserGroupInformation.getLoginUser()).andStubReturn(loginUser);

    currentUser.set(loginUser);
    expect(UserGroupInformation.getCurrentUser()).andStubAnswer(currentUser::get);

    final Capture<String> proxyNameCapture = newCapture();
    final Capture<UserGroupInformation> realCapture = newCapture();
    expect(UserGroupInformation.createProxyUser(capture(proxyNameCapture), capture(realCapture)))
        .andStubAnswer(() -> stubUser(proxyNameCapture.getValue(), realCapture.getValue()));

    Capture<String> remoteNameCapture = newCapture();
    expect(UserGroupInformation.createRemoteUser(capture(remoteNameCapture)))
        .andStubAnswer(() -> stubUser(remoteNameCapture.getValue(), null));
    expect(UserGroupInformation.isSecurityEnabled()).andReturn(false).anyTimes();
  }

  private static UserGroupInformation stubUser(@Nonnull String name, UserGroupInformation realUser) throws IOException, InterruptedException {
    Objects.requireNonNull(name, "Null user");
    final UserGroupInformation mock = mock(UserGroupInformation.class);
    expect(mock.getUserName()).andStubReturn(name);
    expect(mock.getRealUser()).andStubReturn(realUser == null ? mock : realUser);
    final Capture<PrivilegedAction<Object>> actionCapture = newCapture();
    expect(mock.doAs(capture(actionCapture)))
        .andStubAnswer(runAsUser(mock, () -> actionCapture.getValue().run()));
    final Capture<PrivilegedExceptionAction<Object>> actionWithExceptionCapture = newCapture();
    expect(mock.doAs(capture(actionWithExceptionCapture)))
        .andStubAnswer(runAsUser(mock, () -> actionWithExceptionCapture.getValue().run()));
    replay(mock);
    return mock;
  }

  private static IAnswer<Object> runAsUser(UserGroupInformation mock, Callable<Object> action) {
    return () -> {
      final UserGroupInformation previous = currentUser.getAndSet(mock);
      try {
        return action.call();
      } finally {
        currentUser.set(previous);
      }
    };
  }
}
