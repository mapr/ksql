/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.util;

import com.mapr.fs.MapRFileAce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.streams.mapr.Utils;

import java.io.IOException;
import java.util.ArrayList;

public class MaprFSUtils {

  public static void createAppDirAndInternalStreamsIfNotExist(KsqlConfig config) {
    try {
      FileSystem fs = FileSystem.get(new Configuration());
      String currentUser = UserGroupInformation.getCurrentUser().getUserName();
      if (!Utils.maprFSpathExists(fs, KsqlConfig.KSQL_SERVICES_COMMON_FOLDER)) {
        throw new KsqlException(KsqlConfig.KSQL_SERVICES_COMMON_FOLDER + " doesn't exist");
      }
      if (!Utils.maprFSpathExists(fs, config.getCommandsStreamFolder())) {
        // Creation of application forler with appropriate aces
        ArrayList<MapRFileAce> aceList = new ArrayList<MapRFileAce>();

        MapRFileAce ace = new MapRFileAce(MapRFileAce.AccessType.READDIR);
        ace.setBooleanExpression("u:" + currentUser);
        aceList.add(ace);
        ace = new MapRFileAce(MapRFileAce.AccessType.ADDCHILD);
        ace.setBooleanExpression("u:" + currentUser);
        aceList.add(ace);
        ace = new MapRFileAce(MapRFileAce.AccessType.LOOKUPDIR);
        ace.setBooleanExpression("u:" + currentUser);
        aceList.add(ace);
        ace = new MapRFileAce(MapRFileAce.AccessType.DELETECHILD);
        ace.setBooleanExpression("u:" + currentUser);
        aceList.add(ace);

        Utils.maprFSpathCreate(fs, config.getCommandsStreamFolder(), aceList);
      } else {
        String errorMessage =
                String.format("User: %s has no permissions to run KSQL service with ID: %s",
                currentUser,
                config.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG));
        Utils.validateDirectoryPerms(fs, config.getCommandsStreamFolder(),
                currentUser, errorMessage);
      }
      if (!Utils.streamExists(config.getCommandsStream())) {
        Utils.createStream(config.getCommandsStream(), false);
      }
    }catch (IOException e) {
      throw new KafkaException(e);
    }
  }
}
