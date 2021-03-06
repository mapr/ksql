/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.function.udf.datetime;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.sql.Time;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

@UdfDescription(
    name = "timeadd",
    category = FunctionCategory.DATE_TIME,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "Adds a duration to a TIME value."
)
public class TimeAdd {

  @Udf(description = "Adds a duration to a time")
  public Time timeAdd(
      @UdfParameter(description = "A unit of time, for example SECOND or HOUR") final TimeUnit unit,
      @UdfParameter(description = "An integer number of intervals to add") final Integer interval,
      @UdfParameter(description = "A TIME value.") final Time time
  ) {
    if (unit == null || interval == null || time == null) {
      return null;
    }
    final long nanoResult = LocalTime.ofNanoOfDay(time.getTime() * 1000_000)
        .plus(unit.toNanos(interval), ChronoUnit.NANOS)
        .toNanoOfDay();
    return new Time(TimeUnit.NANOSECONDS.toMillis(nanoResult));
  }
}