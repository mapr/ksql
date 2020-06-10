/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.api.impl;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.api.server.KsqlApiException;
import io.confluent.ksql.api.server.ServerUtils;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlValueCoercer;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

public final class KeyValueExtractor {

  private KeyValueExtractor() {
  }

  public static Struct extractKey(final JsonObject values, final LogicalSchema logicalSchema,
      final SqlValueCoercer sqlValueCoercer) {
    final Struct key = new Struct(logicalSchema.keyConnectSchema());
    for (final Field field : key.schema().fields()) {
      final Object value = values.getValue(field.name());
      if (value == null) {
        throw new KsqlApiException("Key field must be specified: " + field.name(),
            Errors.ERROR_CODE_BAD_REQUEST);
      }
      final Object coercedValue = coerceObject(value,
          SchemaConverters.connectToSqlConverter().toSqlType(field.schema()),
          sqlValueCoercer);
      key.put(field, coercedValue);
    }
    return key;
  }

  public static GenericRow extractValues(final JsonObject values, final LogicalSchema logicalSchema,
      final SqlValueCoercer sqlValueCoercer) {
    final List<Column> valColumns = logicalSchema.value();
    final List<Object> vals = new ArrayList<>(valColumns.size());
    for (Column column : valColumns) {
      final String colName = column.name().text();
      final Object val = values.getValue(colName);
      final Object coercedValue =
          val == null ? null : coerceObject(val, column.type(), sqlValueCoercer);
      vals.add(coercedValue);
    }
    return GenericRow.fromList(vals);
  }

  static JsonObject convertColumnNameCase(final JsonObject jsonObjectWithCaseInsensitiveFields) {
    final JsonObject jsonObject = new JsonObject();

    for (Map.Entry<String, Object> entry :
        jsonObjectWithCaseInsensitiveFields.getMap().entrySet()) {
      final String key;
      try {
        key = ServerUtils.getIdentifierText(entry.getKey());
      } catch (IllegalArgumentException e) {
        throw new KsqlApiException(
            String.format("Invalid column name. Column: %s. Reason: %s",
                entry.getKey(), e.getMessage()),
            Errors.ERROR_CODE_BAD_REQUEST
        );
      }

      jsonObject.put(key, entry.getValue());
    }

    return jsonObject;
  }

  private static Object coerceObject(
      final Object value,
      final SqlType sqlType,
      final SqlValueCoercer sqlValueCoercer
  ) {
    if (sqlType instanceof SqlDecimal) {
      // We have to handle this manually as SqlValueCoercer doesn't seem to do it
      final SqlDecimal decType = (SqlDecimal) sqlType;
      if (value instanceof Double) {
        return new BigDecimal(String.valueOf(value))
            .setScale(decType.getScale(), RoundingMode.HALF_UP);
      } else if (value instanceof String) {
        return new BigDecimal((String) value).setScale(decType.getScale(), RoundingMode.HALF_UP);
      } else if (value instanceof Integer) {
        return new BigDecimal((Integer) value).setScale(decType.getScale(), RoundingMode.HALF_UP);
      } else if (value instanceof Long) {
        return new BigDecimal((Long) value).setScale(decType.getScale(), RoundingMode.HALF_UP);
      }
    }
    final Object rawValue;
    if (value instanceof JsonArray) {
      rawValue = ((JsonArray) value).getList();
    } else if (value instanceof JsonObject) {
      rawValue = ((JsonObject) value).getMap();
    } else {
      rawValue = value;
    }
    return sqlValueCoercer.coerce(rawValue, sqlType)

        .orElseThrow(() -> new KsqlApiException(
            String.format("Can't coerce a field of type %s (%s) into type %s", rawValue.getClass(),
                rawValue, sqlType),
            Errors.ERROR_CODE_BAD_REQUEST))
        .orElse(null);
  }

}
