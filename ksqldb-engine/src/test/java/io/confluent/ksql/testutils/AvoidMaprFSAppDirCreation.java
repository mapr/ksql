package io.confluent.ksql.testutils;

import io.confluent.ksql.util.MaprFSUtils;
import org.apache.kafka.streams.mapr.InternalStorageManager;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.spi.PowerMockPolicy;
import org.powermock.mockpolicies.MockPolicyClassLoadingSettings;
import org.powermock.mockpolicies.MockPolicyInterceptionSettings;

public class AvoidMaprFSAppDirCreation implements PowerMockPolicy {

  @Override
  public void applyClassLoadingPolicy(MockPolicyClassLoadingSettings settings) {
    settings.addFullyQualifiedNamesOfClassesToLoadByMockClassloader(
        MaprFSUtils.class.getName(),
        InternalStorageManager.class.getName()
    );
  }

  @Override
  public void applyInterceptionPolicy(MockPolicyInterceptionSettings settings) {
    PowerMock.mockStaticPartial(MaprFSUtils.class,
        "createAppDirAndInternalStreamsIfNotExist");
    PowerMock.mockStaticPartial(InternalStorageManager.class,
        "create");
  }
}
