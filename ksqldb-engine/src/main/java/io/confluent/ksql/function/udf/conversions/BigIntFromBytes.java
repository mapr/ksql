/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udf.conversions;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.BytesUtils;
import io.confluent.ksql.util.KsqlConstants;
import java.nio.ByteBuffer;

@UdfDescription(
    name = "int_from_bytes",
    category = FunctionCategory.CONVERSIONS,
    description = "Converts a BYTES value to a BIGINT type.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class BigIntFromBytes {
  private static final int BYTES_LENGTH = 8;

  @Udf(description = "Converts a BYTES value to a BIGINT type.")
  public Long bigIntFromBytes(
      @UdfParameter(description = "The bytes value to convert.") final ByteBuffer value
  ) {
    if (value == null) {
      return null;
    }

    if (BytesUtils.getByteArray(value).length != BYTES_LENGTH) {
      return null;
    }

    value.rewind();
    return value.getLong();
  }
}
