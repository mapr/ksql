/**
 * Copyright 2017 Confluent Inc.
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

import java.net.ConnectException;
import java.util.Map;
import java.util.stream.Collectors;

public class CommonUtils {
  public static String getErrorMessageWithCause(Throwable e) {
    String msg = getErrorMessage(e);
    String causeMsg = getErrorCauseMessage(e);
    // append the cause msg, if any
    return causeMsg.isEmpty() ? msg : msg + "\r\n" + causeMsg;
  }

  public static String getErrorMessage(Throwable e) {
    if (e instanceof ConnectException) {
      return "Could not connect to the server.";
    } else {
      return e.getMessage();
    }
  }

  public static String getErrorCauseMessage(Throwable e) {
    String prefix = "Caused by: ";
    String msg = "";
    // walk down the cause stack and append error messages
    e = e.getCause();
    while (e != null) {
      msg += prefix + getErrorMessage(e);
      e = e.getCause();
      prefix = "\r\n" + prefix;
    }
    return msg;
  }

  public static Map<String, Object> getPropertiesWithoutPrefix(
      String prefix, Map<String, Object> properties) {
    return properties.keySet()
        .stream()
        .filter(s -> !s.startsWith(prefix))
        .collect(Collectors.toMap(s -> s, s -> properties.get(s)));
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
