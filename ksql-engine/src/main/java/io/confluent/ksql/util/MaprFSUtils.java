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

    public static String decorateTopicWithDefaultStreamIfNeeded(String topic, String defaultStream){
        return topic.contains(":") ? topic : decorateTopicWithDefaultStream(topic, defaultStream);
    }

    private static String decorateTopicWithDefaultStream(String topic, String defaultStream){
        if(defaultStream.isEmpty()){
            throw new KsqlException("Cannot decorate topic with default stream. " +
                    "Set " + KsqlConfig.KSQL_DEFAULT_STREAM_CONFIG);
        }
        return String.format("%s:%s", defaultStream, topic);
    }
}
