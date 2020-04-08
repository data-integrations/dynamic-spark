/*
 * Copyright © 2018-2019 Cask Data, Inc.
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
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.plugin.common.LineageRecorder;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * A {@link SparkSink} that takes any scala code and executes it.
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("ScalaSparkSink")
@Description("Executes user-provided Spark code written in Scala that performs RDD operations")
public class ScalaSparkSink extends SparkSink<StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(ScalaSparkSink.class);

  private final transient Config config;
  // A strong reference is needed to keep the compiled classes around
  @SuppressWarnings("FieldCanBeLocal")
  private transient ScalaSparkCodeExecutor codeExecutor;

  public ScalaSparkSink(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    if (!config.containsMacro("scalaCode") && !config.containsMacro("dependencies")
      && Boolean.TRUE.equals(config.getDeployCompile())) {
      codeExecutor = new ScalaSparkCodeExecutor(config.getScalaCode(), config.getDependencies(), "sink", true);
      codeExecutor.configure(stageConfigurer.getInputSchema());
    }
  }

  @Override
  public void prepareRun(SparkPluginContext sparkPluginContext) throws Exception {
    Schema schema = sparkPluginContext.getInputSchema();
    if (schema != null && schema.getFields() != null) {
      recordLineage(sparkPluginContext, config.referenceName, schema, "Write", "Wrote to Scala Spark Sink.");
    }
  }

  @Override
  public void run(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> javaRDD) throws Exception {
    codeExecutor = new ScalaSparkCodeExecutor(config.getScalaCode(), config.getDependencies(), "sink", true);
    codeExecutor.initialize(context);
    codeExecutor.execute(context, javaRDD);
  }

  /**
   * Configuration object for the plugin
   */
  public static final class Config extends PluginConfig {

    @Description("Spark code in Scala defining what operations to perform. " +
      "The code must implement a function " +
      "called 'sink', which has signature as either \n" +
      "  def sink(rdd: RDD[StructuredRecord]) : Unit\n" +
      "  or\n" +
      "  def sink(rdd: RDD[StructuredRecord], context: SparkExecutionPluginContext) : Unit\n" +
      "For example:\n" +
      "'def transform(rdd: RDD[StructuredRecord]) : Unit = {\n" +
      "   rdd.filter(_.get(\"gender\") == null).saveAsTextFile(\"output\")\n" +
      " }'\n" +
      "will filter out incoming records that does not have the 'gender' field and write the results as a text file."
    )
    @Macro
    private final String scalaCode;

    @Description(
      "Extra dependencies for the Spark program. " +
        "It is a ',' separated list of URI for the location of dependency jars. " +
        "A path can be ended with an asterisk '*' as a wildcard, in which all files with extension '.jar' under the " +
        "parent path will be included."
    )
    @Macro
    @Nullable
    private final String dependencies;

    @Description("Decide whether to perform code compilation at deployment time. It will be useful to turn it off " +
      "in cases when some library classes are only available at run time, but not at deployment time.")
    @Nullable
    private final Boolean deployCompile;

    @Name("referenceName")
    @Description("This will be used to uniquely identify this source/sink for lineage, annotating metadata, etc.")
    public String referenceName;

    public Config(String scalaCode, @Nullable String dependencies, @Nullable Boolean deployCompile,
                  String referenceName) {
      this.scalaCode = scalaCode;
      this.dependencies = dependencies;
      this.deployCompile = deployCompile;
      this.referenceName = referenceName;
    }

    public String getScalaCode() {
      return scalaCode;
    }

    @Nullable
    public String getDependencies() {
      return dependencies;
    }

    @Nullable
    public Boolean getDeployCompile() {
      return deployCompile;
    }

    public String getReferenceName() {
      return referenceName;
    }
  }

  private void recordLineage(SparkPluginContext context, String outputName, Schema tableSchema, String operationName,
                             String description) {
    LineageRecorder lineageRecorder = new LineageRecorder(context, outputName);
    lineageRecorder.createExternalDataset(tableSchema);
    List<String> fieldNames = tableSchema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList());
    if (!fieldNames.isEmpty()) {
      lineageRecorder.recordWrite(operationName, description, fieldNames);
    }
  }
}
