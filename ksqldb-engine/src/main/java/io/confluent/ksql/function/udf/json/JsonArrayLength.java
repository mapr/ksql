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

package io.confluent.ksql.function.udf.json;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;

@UdfDescription(
    name = "JSON_ARRAY_LENGTH",
    category = FunctionCategory.JSON,
    description = "Given a string, parses it as a JSON value and returns the length of the "
        + "top-level array. Returns NULL if the string can't be interpreted as a JSON array, "
        + "for example, when the string `NULL` or the JSON value is not an array. This function"
        + "throws `Invalid JSON format` if the string can't be interpreted as a JSON value.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class JsonArrayLength {

  private static final ObjectReader OBJECT_READER = UdfJsonMapper.INSTANCE.get().reader();

  @Udf
  public Integer length(@UdfParameter final String jsonArray) {
    if (jsonArray == null) {
      return null;
    }

    final JsonNode node = parseJson(jsonArray);
    if (node.isMissingNode() || !node.isArray()) {
      return null;
    }

    return node.size();
  }

  private static JsonNode parseJson(final String jsonString) {
    try {
      return OBJECT_READER.readTree(jsonString);
    } catch (final JacksonException e) {
      throw new KsqlFunctionException("Invalid JSON format:" + jsonString, e);
    }
  }
}
