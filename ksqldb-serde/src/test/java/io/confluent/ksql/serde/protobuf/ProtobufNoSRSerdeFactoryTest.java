/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.serde.protobuf;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.serde.SerdeFactory;
import io.confluent.ksql.test.util.UserGroupInformationMockPolicy;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.powermock.core.classloader.annotations.MockPolicy;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@MockPolicy(UserGroupInformationMockPolicy.class)
@PowerMockIgnore("javax.net.ssl.*")
public class ProtobufNoSRSerdeFactoryTest extends AbstractProtobufSerdeFactoryTest {

  @Override
  SerdeFactory getSerdeFactory() {
    return new ProtobufSerdeFactory(ImmutableMap.of());
  }
}