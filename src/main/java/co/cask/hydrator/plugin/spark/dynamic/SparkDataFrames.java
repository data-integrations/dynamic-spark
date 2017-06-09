/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.hydrator.plugin.spark.dynamic;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Util class to for supporting DataFrame with Structured record
 */
public final class SparkDataFrames {

  private SparkDataFrames() {
    // private
  }

  /**
   * Creates the Spark {@link StructType} that represents the given record type {@link Schema}.
   */
  public static StructType toStructType(Schema schema) {
    if (schema.getType() != Schema.Type.RECORD) {
      throw new IllegalArgumentException("Only record type schema is supported");
    }
    // The casting should work since we checked the type is RECORD
    return (StructType) toDataType(schema);
  }

  /**
   * Creates a {@link Schema} object from the given {@link StructType}.
   */
  public static Schema toSchema(StructType structType) {
    return toSchema(structType, new int[] { 0 });
  }

  /**
   * Creates a {@link Row} object that represents data in the given {@link StructuredRecord}.
   *
   * @param record contains the record data
   * @param structType a {@link StructType} representing the data type in the resulting {@link Row}.
   * @return a new {@link Row} instance
   */
  public static Row toRow(StructuredRecord record, StructType structType) {
    return (Row) toRowValue(record, structType);
  }

  /**
   * Creates a {@link StructuredRecord} from the data in the given {@link Row}.
   *
   * @param row contains the record data
   * @param schema the {@link Schema} of the resulting {@link StructuredRecord}.
   * @return a new {@link StructuredRecord} instance
   */
  public static StructuredRecord fromRow(Row row, Schema schema) {
    if (schema.getType() != Schema.Type.RECORD) {
      throw new IllegalArgumentException("Only record type schema is supported");
    }
    return (StructuredRecord) fromRowValue(row, schema);
  }


  /**
   * Converts a {@link Schema} to Spark {@link DataType}.
   */
  private static DataType toDataType(Schema schema) {
    switch (schema.getType()) {
      case NULL:
        return DataTypes.NullType;
      case BOOLEAN:
        return DataTypes.BooleanType;
      case INT:
        return DataTypes.IntegerType;
      case LONG:
        return DataTypes.LongType;
      case FLOAT:
        return DataTypes.FloatType;
      case DOUBLE:
        return DataTypes.DoubleType;
      case BYTES:
        return DataTypes.BinaryType;
      case STRING:
        return DataTypes.StringType;
      case ENUM:
        return DataTypes.StringType;
      case ARRAY:
        Schema componentSchema = schema.getComponentSchema();
        return DataTypes.createArrayType(toDataType(componentSchema), componentSchema.isNullable());
      case MAP:
        Map.Entry<Schema, Schema> mapSchema = schema.getMapSchema();
        return DataTypes.createMapType(toDataType(mapSchema.getKey()),
                                       toDataType(mapSchema.getValue()), mapSchema.getValue().isNullable());
      case RECORD:
        List<StructField> structFields = new ArrayList<>();
        for (Schema.Field field : schema.getFields()) {
          Schema fieldSchema = field.getSchema();
          DataType fieldType = toDataType(fieldSchema);
          structFields.add(DataTypes.createStructField(field.getName(), fieldType, fieldSchema.isNullable()));
        }
        return DataTypes.createStructType(structFields);
      case UNION:
        if (schema.isNullable()) {
          return toDataType(schema.getNonNullable());
        }
    }
    throw new IllegalArgumentException("Unsupported schema: " + schema);
  }

  /**
   * Converts a Spark {@link DataType} to a {@link Schema} object.
   *
   * @param dataType the data type to convert from
   * @param recordCounter tracks number of record schema becoming created; used for record name generation only
   * @return a new {@link Schema}.
   */
  private static Schema toSchema(DataType dataType, int[] recordCounter) {
    if (dataType.equals(DataTypes.NullType)) {
      return Schema.of(Schema.Type.NULL);
    }
    if (dataType.equals(DataTypes.BooleanType)) {
      return Schema.of(Schema.Type.BOOLEAN);
    }
    if (dataType.equals(DataTypes.ByteType)) {
      return Schema.of(Schema.Type.INT);
    }
    if (dataType.equals(DataTypes.ShortType)) {
      return Schema.of(Schema.Type.INT);
    }
    if (dataType.equals(DataTypes.IntegerType)) {
      return Schema.of(Schema.Type.INT);
    }
    if (dataType.equals(DataTypes.LongType)) {
      return Schema.of(Schema.Type.LONG);
    }
    if (dataType.equals(DataTypes.FloatType)) {
      return Schema.of(Schema.Type.FLOAT);
    }
    if (dataType.equals(DataTypes.DoubleType)) {
      return Schema.of(Schema.Type.DOUBLE);
    }
    if (dataType.equals(DataTypes.BinaryType)) {
      return Schema.of(Schema.Type.BYTES);
    }
    if (dataType.equals(DataTypes.StringType)) {
      return Schema.of(Schema.Type.STRING);
    }
    if (dataType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) dataType;
      Schema componentSchema = toSchema(arrayType.elementType(), recordCounter);
      return Schema.arrayOf(arrayType.containsNull() ? Schema.nullableOf(componentSchema) : componentSchema);
    }
    if (dataType instanceof MapType) {
      MapType mapType = (MapType) dataType;
      Schema valueSchema = toSchema(mapType.valueType(), recordCounter);
      return Schema.mapOf(toSchema(mapType.keyType(), recordCounter),
                          mapType.valueContainsNull() ? Schema.nullableOf(valueSchema) : valueSchema);
    }
    if (dataType instanceof StructType) {
      List<Schema.Field> fields = new ArrayList<>();
      for (StructField structField : ((StructType) dataType).fields()) {
        Schema fieldSchema = toSchema(structField.dataType(), recordCounter);
        fields.add(Schema.Field.of(structField.name(),
                                   structField.nullable() ? Schema.nullableOf(fieldSchema) : fieldSchema));
      }
      return Schema.recordOf("Record" + recordCounter[0]++, fields);
    }

    // Some special types in Spark SQL
    if (dataType.equals(DataTypes.TimestampType)) {
      return Schema.of(Schema.Type.LONG);
    }
    if (dataType.equals(DataTypes.DateType)) {
      return Schema.of(Schema.Type.LONG);
    }

    // Not support the CalendarInterval type for now, as there is no equivalent in Schema
    throw new IllegalArgumentException("Unsupported data type: " + dataType.typeName());
  }

  /**
   * Converts a value from {@link StructuredRecord} to a value acceptable by {@link Row}
   *
   * @param value the value to convert
   * @param dataType the target {@link DataType} of the value
   * @return an object that is compatible with Spark {@link Row}.
   */
  private static Object toRowValue(Object value, DataType dataType) {
    if (dataType.equals(DataTypes.NullType)) {
      return null;
    }
    if (dataType.equals(DataTypes.BooleanType)) {
      return value;
    }
    if (dataType.equals(DataTypes.ByteType)) {
      return value;
    }
    if (dataType.equals(DataTypes.ShortType)) {
      return value;
    }
    if (dataType.equals(DataTypes.IntegerType)) {
      return value;
    }
    if (dataType.equals(DataTypes.LongType)) {
      return value;
    }
    if (dataType.equals(DataTypes.FloatType)) {
      return value;
    }
    if (dataType.equals(DataTypes.DoubleType)) {
      return value;
    }
    if (dataType.equals(DataTypes.BinaryType)) {
      if (value instanceof ByteBuffer) {
        // If it is backed by array and of the same size, just return it
        ByteBuffer buffer = (ByteBuffer) value;
        if (buffer.hasArray() && buffer.array().length == buffer.remaining()) {
          return buffer.array();
        }
        return Bytes.toBytes((ByteBuffer) value);
      }
      return value;
    }
    if (dataType.equals(DataTypes.StringType)) {
      return value;
    }
    if (dataType instanceof ArrayType) {
      @SuppressWarnings("unchecked")
      Collection<Object> collection = (Collection<Object>) value;
      List<Object> result = new ArrayList<>(collection.size());
      for (Object obj : collection) {
        result.add(toRowValue(obj, ((ArrayType) dataType).elementType()));
      }
      return result;
    }
    if (dataType instanceof MapType) {
      @SuppressWarnings("unchecked")
      Map<Object, Object> map = (Map<Object, Object>) value;
      Map<Object, Object> result = new LinkedHashMap<>(map.size());
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        result.put(toRowValue(entry.getKey(), ((MapType) dataType).keyType()),
                   toRowValue(entry.getValue(), ((MapType) dataType).valueType()));
      }
      return result;
    }
    if (dataType instanceof StructType) {
      StructuredRecord record = (StructuredRecord) value;
      StructField[] fields = ((StructType) dataType).fields();
      Object[] fieldValues = new Object[fields.length];
      for (int i = 0; i < fields.length; i++) {
        fieldValues[i] = toRowValue(record.get(fields[i].name()), fields[i].dataType());
      }
      return RowFactory.create(fieldValues);
    }

    // Some special types in Spark SQL
    if (dataType.equals(DataTypes.TimestampType)) {
      return new Timestamp((long) value);
    }
    if (dataType.equals(DataTypes.DateType)) {
      return new Date((long) value);
    }

    // Not support the CalendarInterval type for now, as there is no equivalent in Schema
    throw new IllegalArgumentException("Unsupported data type: " + dataType.typeName());
  }

  /**
   * Converts a value from Spark {@link Row} into value acceptable for {@link StructuredRecord}.
   *
   * @param value the value to convert from
   * @param schema the target {@link Schema} of the value
   * @return a value object acceptable to be used in {@link StructuredRecord}.
   */
  private static Object fromRowValue(Object value, Schema schema) {
    switch (schema.getType()) {
      // For all simple types, return as is.
      case NULL:
        return null;
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
        return value;
      case BYTES:
        return ByteBuffer.wrap((byte[]) value);
      case ARRAY: {
        // Value must be a collection
        @SuppressWarnings("unchecked")
        Collection<Object> collection = (Collection<Object>) value;
        List<Object> result = new ArrayList<>(collection.size());
        for (Object element : collection) {
          result.add(fromRowValue(element, schema.getComponentSchema()));
        }
        return result;
      }
      case MAP: {
        // Value must be a Map
        Map<?, ?> map = (Map<?, ?>) value;
        Map<Object, Object> result = new LinkedHashMap<>(map.size());
        Map.Entry<Schema, Schema> mapSchema = schema.getMapSchema();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          result.put(fromRowValue(entry.getKey(), mapSchema.getKey()),
                     fromRowValue(entry.getValue(), mapSchema.getValue()));
        }
        return result;
      }
      case RECORD: {
        // Value must be a Row
        Row row = (Row) value;
        StructuredRecord.Builder builder = StructuredRecord.builder(schema);
        int idx = 0;
        for (Schema.Field field : schema.getFields()) {
          Schema fieldSchema = field.getSchema();

          if (row.isNullAt(idx) && !fieldSchema.isNullable()) {
            throw new NullPointerException("Null value is not allowed in record field "
                                             + schema.getRecordName() + "." + field.getName());
          }

          fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;

          // If the value is null for the field, just continue without setting anything to the StructuredRecord
          if (row.isNullAt(idx)) {
            continue;
          }

          // Special case handling for ARRAY and MAP in order to get the Java type
          if (fieldSchema.getType() == Schema.Type.ARRAY) {
            builder.set(field.getName(), fromRowValue(row.getList(idx), fieldSchema));
          } else if (fieldSchema.getType() == Schema.Type.MAP) {
            builder.set(field.getName(), fromRowValue(row.getJavaMap(idx), fieldSchema));
          } else {
            Object fieldValue = row.get(idx);

            // Date and timestamp special return type handling
            if (fieldValue instanceof Date) {
              fieldValue = ((Date) fieldValue).getTime();
            } else if (fieldValue instanceof Timestamp) {
              fieldValue = ((Timestamp) fieldValue).getTime();
            }
            builder.set(field.getName(), fromRowValue(fieldValue, fieldSchema));
          }

          idx++;
        }
        return builder.build();
      }
    }

    throw new IllegalArgumentException("Unsupported schema: " + schema);
  }
}
