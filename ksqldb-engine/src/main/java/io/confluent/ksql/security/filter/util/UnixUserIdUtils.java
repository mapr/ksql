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

package io.confluent.ksql.security.filter.util;

import com.google.common.base.Suppliers;
import java.io.IOException;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.IdMappingServiceProvider;
import org.apache.hadoop.security.ShellBasedIdMapping;
import org.apache.kafka.common.KafkaException;

public final class UnixUserIdUtils {
  private UnixUserIdUtils() {
  }

  private static final Supplier<IdMappingServiceProvider> LAZY_UNIX_ID_MAPPER
      = Suppliers.memoize(UnixUserIdUtils::createShellBasedIdMapping)::get;

  private static ShellBasedIdMapping createShellBasedIdMapping() {
    try {
      return new ShellBasedIdMapping(new Configuration());
    } catch (IOException e) {
      throw new KafkaException("Cannot initialize ShellBasedIdMapping mapper");
    }
  }

  public static IdMappingServiceProvider getUnixIdMapper() {
    return LAZY_UNIX_ID_MAPPER.get();
  }
}
