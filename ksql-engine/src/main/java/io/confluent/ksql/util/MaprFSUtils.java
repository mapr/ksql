package io.confluent.ksql.util;

import com.mapr.fs.MapRFileAce;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.streams.mapr.Utils;

import java.io.IOException;
import java.util.ArrayList;

public class MaprFSUtils {

    public static void createAppDirAndInternalStreamsIfNotExist(KsqlConfig config) {
        try {
            if (!Utils.maprFSpathExists(KsqlConfig.KSQL_SERVICES_COMMON_FOLDER)) {
                throw new KsqlException(KsqlConfig.KSQL_SERVICES_COMMON_FOLDER + " doesn't exist");
            }
            if (!Utils.maprFSpathExists(config.getCommandsStreamFolder())) {
                // Creation of application forler with appropriate aces
                String currentUser = System.getProperty("user.name");
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

                Utils.maprFSpathCreate(config.getCommandsStreamFolder(), aceList);
            }
            if (!Utils.streamExists(config.getCommandsStream())) {
                Utils.createStream(config.getCommandsStream(), false);
            }
        }catch (IOException e) {
            throw new KafkaException(e);
        }
    }
}
