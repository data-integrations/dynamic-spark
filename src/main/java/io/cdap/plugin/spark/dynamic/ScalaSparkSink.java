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
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    FailureCollector collector = stageConfigurer.getFailureCollector();
    if (!config.containsMacro("scalaCode") && !config.containsMacro("dependencies")
      && Boolean.TRUE.equals(config.getDeployCompile())) {
      codeExecutor = new ScalaSparkCodeExecutor(config.getScalaCode(), config.getDependencies(), "sink", true);
      try {
        codeExecutor.configure(stageConfigurer.getInputSchema());
      } catch (Exception e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(Config.SCALA_CODE)
          .withStacktrace(e.getStackTrace());
      }
    }
  }

  @Override
  public void prepareRun(SparkPluginContext sparkPluginContext) throws Exception {
    // no-op
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
    private static final String SCALA_CODE = "scalaCode";

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

    public Config(String scalaCode, @Nullable String dependencies,
                  @Nullable Boolean deployCompile) {
      this.scalaCode = scalaCode;
      this.dependencies = dependencies;
      this.deployCompile = deployCompile;
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
  }
}
