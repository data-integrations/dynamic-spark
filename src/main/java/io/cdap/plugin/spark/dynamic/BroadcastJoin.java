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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Performs a broadcast join.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(BroadcastJoin.NAME)
@Description("Performs a broadcast join by loading a small dataset into memory.")
public class BroadcastJoin extends SparkCompute<StructuredRecord, StructuredRecord> {
  public static final String NAME = "BroadcastJoin";
  private static final Set<Schema.Type> SUPPORTED_TYPES =
    new HashSet<>(Arrays.asList(Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE,
                                Schema.Type.BOOLEAN, Schema.Type.STRING));
  private final BroadcastJoinConfig config;

  public BroadcastJoin(BroadcastJoinConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    if (inputSchema == null) {
      stageConfigurer.setOutputSchema(null);
      return;
    }

    FailureCollector failureCollector = stageConfigurer.getFailureCollector();
    List<DatasetJoinInfo> datasetJoins = config.getDatasetsToJoin();
    Schema schema = inputSchema;
    for (DatasetJoinInfo joinInfo : datasetJoins) {
      for (String joinKey : joinInfo.getJoinKeys()) {
        if (schema.getField(joinKey) == null) {
          failureCollector.addFailure("Join key does not exist in the schema.", "")
            .withConfigElement("joinOn" + joinInfo.getNum(), joinKey);
        }
      }
      schema = joinSchemas(schema, joinInfo.getSchema(), joinInfo.getJoinKeys());
    }
    stageConfigurer.setOutputSchema(schema);
  }

  private Schema joinSchemas(Schema schema1, Schema schema2, Set<String> joinKeys) {
    List<Schema.Field> fields =
      new ArrayList<>(schema1.getFields().size() + schema2.getFields().size() - joinKeys.size());
    fields.addAll(schema1.getFields());
    for (Schema.Field field : schema2.getFields()) {
      if (joinKeys.contains(field.getName())) {
        continue;
      }
      if (field.getSchema().isNullable()) {
        fields.add(field);
      } else {
        fields.add(Schema.Field.of(field.getName(), Schema.nullableOf(field.getSchema())));
      }
    }
    return Schema.recordOf("joined", fields);
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context,
                                             JavaRDD<StructuredRecord> javaRDD) {
    BroadcastJoiner joiner = new BroadcastJoiner(config);
    return joiner.join(context, javaRDD);
  }

}
