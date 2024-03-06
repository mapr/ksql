/**
 * Copyright 2018 Confluent Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at</p>
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0<p></p>
 *
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.</p>
 **/

package io.confluent.ksql.util;

import static org.apache.kafka.mapr.tools.KafkaMaprStreams.PUBLIC_PERMISSIONS;

import com.mapr.fs.MapRFileAce;
import io.confluent.ksql.logging.processing.ProcessingLogConfig;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.PermissionNotMatchException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.mapr.tools.KafkaMaprStreams;
import org.apache.kafka.mapr.tools.KafkaMaprTools;
import org.apache.kafka.mapr.tools.KafkaMaprfs;

public final class MaprFSUtils {

  private MaprFSUtils() {
    // Never called in utility class
  }

  public static void createAppDirAndInternalStreamsIfNotExist(final KsqlConfig config) {
    createAppDirIfNotExists(config);
    try (KafkaMaprStreams maprStreams = KafkaMaprTools.tools().streams()) {
      final String streamName = config.getCommandsStream();
      if (!maprStreams.streamExists(streamName)) {
        maprStreams.createStreamForAllUsers(streamName);
      }
      if (!maprStreams.streamHasPerms(streamName, PUBLIC_PERMISSIONS, PUBLIC_PERMISSIONS)) {
        maprStreams.setStreamPerms(streamName, PUBLIC_PERMISSIONS, PUBLIC_PERMISSIONS);
      }
    }
  }

  public static void createAppDirIfNotExists(final KsqlConfig config) {
    try  {
      final KafkaMaprfs fs = KafkaMaprTools.tools().maprfs();
      final String currentUser = UserGroupInformation.getCurrentUser().getUserName();
      if (!fs.exists(KsqlConfig.KSQL_SERVICES_COMMON_FOLDER)) {
        throw new KsqlException(KsqlConfig.KSQL_SERVICES_COMMON_FOLDER + " doesn't exist");
      }
      if (!fs.exists(config.getCommandsStreamFolder())) {
        // Creation of application folder with appropriate ACEs
        final ArrayList<MapRFileAce> aceList = new ArrayList<MapRFileAce>();

        MapRFileAce ace = new MapRFileAce(MapRFileAce.AccessType.READDIR);
        ace.setBooleanExpression("p");
        aceList.add(ace);
        ace = new MapRFileAce(MapRFileAce.AccessType.ADDCHILD);
        ace.setBooleanExpression("u:" + currentUser);
        aceList.add(ace);
        ace = new MapRFileAce(MapRFileAce.AccessType.LOOKUPDIR);
        ace.setBooleanExpression("p");
        aceList.add(ace);
        ace = new MapRFileAce(MapRFileAce.AccessType.DELETECHILD);
        ace.setBooleanExpression("u:" + currentUser);
        aceList.add(ace);

        fs.mkdirs(config.getCommandsStreamFolder());
        fs.setAces(config.getCommandsStreamFolder(), aceList);
      } else {
        if (!fs.isAccessibleAsDirectory(config.getCommandsStreamFolder())) {
          final String errorMessage =
              String.format("User: %s has no permissions to run KSQL service with ID: %s",
                  currentUser,
                  config.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG));
          throw new KafkaException(new PermissionNotMatchException(errorMessage));
        }
      }
    } catch (IOException e) {
      throw new KafkaException(e);
    }
  }

  public static void createAppDirAndProceccingLogStreamIfNotExists(
      final ProcessingLogConfig config,
      final KsqlConfig ksqlConfig
  ) {
    createAppDirIfNotExists(ksqlConfig);
    try (KafkaMaprStreams maprStreams = KafkaMaprTools.tools().streams()) {
      final String streamName = ksqlConfig.getCommandsStreamFolder()
          + config.getString(ProcessingLogConfig.STREAM_NAME);
      if (!maprStreams.streamExists(streamName)) {
        maprStreams.createStreamForAllUsers(streamName);
      }
      if (!maprStreams.streamHasPerms(streamName, PUBLIC_PERMISSIONS, PUBLIC_PERMISSIONS)) {
        maprStreams.setStreamPerms(streamName, PUBLIC_PERMISSIONS, PUBLIC_PERMISSIONS);
      }
    }
  }

  public static String decorateTopicWithDefaultStreamIfNeeded(final String topic,
                                                              final String defaultStream) {
    return topic.contains(":") ? topic : decorateTopicWithDefaultStream(topic, defaultStream);
  }

  private static String decorateTopicWithDefaultStream(final String topic,
                                                       final String defaultStream) {
    if (defaultStream.isEmpty()) {
      throw new KsqlException("Cannot decorate topic with default stream. Set "
          + KsqlConfig.KSQL_DEFAULT_STREAM_CONFIG);
    }
    return String.format("%s:%s", defaultStream, topic);
  }

  public static void deleteAppDirAndInternalStream(final String applicationId) throws IOException {
    final Configuration conf = new Configuration();
    final FileSystem fs = FileSystem.get(conf);
    final String appDir = "/apps/kafka-streams/" + applicationId;

    final Path p = new Path(appDir);
    if (fs.exists(p)) {
      fs.delete(p, true);
    }
  }
}
