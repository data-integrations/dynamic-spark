/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.spark.dynamic;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.spark.sql.DataFrames;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * Performs the join. Separate from the plugin to allow validation endpoint to be free of spark classes.
 */
public class BroadcastJoiner implements Serializable {
  private final BroadcastJoinConfig config;

  public BroadcastJoiner(BroadcastJoinConfig config) {
    this.config = config;
  }

  public JavaRDD<StructuredRecord> join(SparkExecutionPluginContext context,
                                        JavaRDD<StructuredRecord> javaRDD) {
    SQLContext sqlContext = new SQLContext(context.getSparkContext());
    Schema inputSchema = context.getInputSchema();
    if (inputSchema == null) {
      inputSchema = javaRDD.take(1).get(0).getSchema();
    }

    StructType inputSparkSchema = DataFrames.toDataType(inputSchema);
    Dataset<Row> inputDataset = sqlContext.createDataFrame(javaRDD.map(r -> DataFrames.toRow(r, inputSparkSchema)),
                                                           inputSparkSchema);

    Schema smallDatasetSchema = config.getSmallDatasetSchema();

    // TODO: replace with .hadoopFile()
    Dataset<Row> smallDataset = sqlContext.createDataFrame(
      context.getSparkContext().textFile(config.getPath())
        .map(line -> {
          /*
            Parse ourselves, can't do:

            Dataset<Row> smallDataset = sqlContext.read()
              .option("delimiter", config.getDelimiter())
              .schema(DataFrames.toDataType(smallDatasetSchema))
              .csv(config.path);

            because multi-character delimiters are not supported
          */

          String[] lineFields = line.split(Pattern.quote(config.getDelimiter()));
          Object[] vals = new Object[lineFields.length];
          Iterator<Schema.Field> fieldIterator = smallDatasetSchema.getFields().iterator();
          for (int i = 0; i < vals.length; i++) {
            String fieldStr = lineFields[i];

            if (!fieldIterator.hasNext()) {
              vals[i] = null;
              continue;
            }

            Schema fieldSchema = fieldIterator.next().getSchema();

            if (fieldStr == null || fieldStr.isEmpty()) {
              vals[i] = null;
              continue;
            }

            Schema.Type fieldType = fieldSchema.getNonNullable().getType();
            switch (fieldType) {
              case STRING:
                vals[i] = fieldStr;
                break;
              case INT:
                vals[i] = Integer.parseInt(fieldStr);
                break;
              case LONG:
                vals[i] = Long.parseLong(fieldStr);
                break;
              case FLOAT:
                vals[i] = Float.parseFloat(fieldStr);
                break;
              case DOUBLE:
                vals[i] = Double.parseDouble(fieldStr);
                break;
              case BOOLEAN:
                vals[i] = Boolean.parseBoolean(fieldStr);
                break;
              default:
                // should never happen, as it should be checked at configure time
            }
          }
          return RowFactory.create(vals);
        }),
      DataFrames.toDataType(smallDatasetSchema));


    Seq<String> joinKeys = JavaConverters.collectionAsScalaIterableConverter(config.getJoinKeys()).asScala().toSeq();
    Dataset<Row> joined = inputDataset.join(functions.broadcast(smallDataset),
                                            joinKeys, config.getJoinType());

    Schema outputSchema = DataFrames.toSchema(joined.schema());
    return joined.javaRDD().map(row -> DataFrames.fromRow(row, outputSchema));
  }

}
