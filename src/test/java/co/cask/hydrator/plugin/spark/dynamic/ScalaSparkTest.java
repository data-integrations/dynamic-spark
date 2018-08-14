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

import co.cask.cdap.api.artifact.ArtifactRange;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.spark.dynamic.SparkInterpreter;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ScalaSparkTest extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final ArtifactId DATAPIPELINE_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "3.2.0");

  @BeforeClass
  public static void setupTest() throws Exception {
    // add the artifact for data pipeline app
    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);

    // add artifact for spark plugins
    Set<ArtifactRange> parents = ImmutableSet.of(
      new ArtifactRange(NamespaceId.DEFAULT.getNamespace(), DATAPIPELINE_ARTIFACT_ID.getArtifact(),
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true,
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true)
    );
    addPluginArtifact(NamespaceId.DEFAULT.artifact("dynamic-spark", "1.0.0"), parents,
                      ScalaSparkCompute.class, ScalaSparkProgram.class, ScalaSparkSink.class);
  }

  @Test
  public void testScalaProgram() throws Exception {
    StringWriter codeWriter = new StringWriter();
    try (PrintWriter printer = new PrintWriter(codeWriter, true)) {
      printer.println("import co.cask.cdap.api.common._");
      printer.println("import co.cask.cdap.api.dataset._");
      printer.println("import co.cask.cdap.api.dataset.lib._");
      printer.println("import co.cask.cdap.api.spark._");
      printer.println("import org.apache.spark._");
      printer.println("class SparkProgram extends SparkMain {");
      printer.println("  override def run(implicit sec:SparkExecutionContext): Unit = {");
      printer.println("    sec.getAdmin()");
      printer.println("       .createDataset(\"kvTable\", classOf[KeyValueTable].getName(), DatasetProperties.EMPTY);");
      printer.println("    val sc = new SparkContext");
      printer.println("    sc");
      printer.println("      .fromStream[String](\"text\")");
      printer.println("      .flatMap(_.split(\"\\\\s+\"))");
      printer.println("      .map((_, 1))");
      printer.println("      .reduceByKey(_ + _)");
      printer.println("      .map(t => (Bytes.toBytes(t._1), Bytes.toBytes(t._2)))");
      printer.println("      .saveAsDataset(\"kvTable\")");
      printer.println("  }");
      printer.println("}");
    }

    // Pipeline configuration
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("action", new ETLPlugin("ScalaSparkProgram", "sparkprogram", ImmutableMap.of(
        "scalaCode", codeWriter.toString(),
        "mainClass", "SparkProgram"
      ))))
      .build();

    // Deploy the pipeline
    ArtifactSummary artifactSummary = new ArtifactSummary(DATAPIPELINE_ARTIFACT_ID.getArtifact(),
                                                          DATAPIPELINE_ARTIFACT_ID.getVersion());
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(artifactSummary, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("ScalaSparkProgramApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    // Create a stream and write to it
    StreamManager streamManager = getStreamManager("text");
    streamManager.createStream();
    for (int i = 0; i < 10; i++) {
      streamManager.send("Line " + i);
    }

    // Run the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // Validate the result
    KeyValueTable kvTable = this.<KeyValueTable>getDataset("kvTable").get();
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(1, Bytes.toInt(kvTable.read(Integer.toString(i))));
    }
    Assert.assertEquals(10, Bytes.toInt(kvTable.read("Line")));
  }

  @Test
  public void testScalaProgramDependency() throws Exception {
    File jarFile = TEMP_FOLDER.newFile("generated.jar");
    try (SparkInterpreter intp = SparkCompilers.createInterpreter()) {
      intp.compile("object SparkConstants {\n val COLLECTION = Array(1, 2, 3, 4, 5) \n }");
      intp.saveAsJar(jarFile);
    }

    // Run a Spark program that reference to SparkConstants.COLLECTION, which comes from dependency jar
    StringWriter codeWriter = new StringWriter();
    try (PrintWriter printer = new PrintWriter(codeWriter, true)) {
      printer.println("import co.cask.cdap.api.spark._");
      printer.println("import org.apache.spark._");
      printer.println("class SparkProgram extends SparkMain {");
      printer.println("  override def run(implicit sec:SparkExecutionContext): Unit = {");
      printer.println("    val sc = new SparkContext");
      printer.println("    sc.parallelize(SparkConstants.COLLECTION).reduce(_ + _)");
      printer.println("  }");
      printer.println("}");
    }

    // Pipeline configuration
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("action", new ETLPlugin("ScalaSparkProgram", "sparkprogram", ImmutableMap.of(
        "scalaCode", codeWriter.toString(),
        "mainClass", "SparkProgram",
        "dependencies", jarFile.getAbsolutePath()
      ))))
      .build();

    // Deploy the pipeline
    ArtifactSummary artifactSummary = new ArtifactSummary(DATAPIPELINE_ARTIFACT_ID.getArtifact(),
                                                          DATAPIPELINE_ARTIFACT_ID.getVersion());
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(artifactSummary, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("ScalaSparkProgramDependencyApp");
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    // Run the pipeline. It should succeed
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);
  }

  @Test
  public void testScalaSparkComputeDataFrame() throws Exception {
    StringWriter codeWriter = new StringWriter();
    try (PrintWriter printer = new PrintWriter(codeWriter, true)) {
      printer.println("def transform(df: DataFrame) : DataFrame = {");
      printer.println("  val splitted = df.explode(\"body\", \"word\") { ");
      printer.println("    line: String => line.split(\"\\\\s+\")");
      printer.println("  }");
      printer.println("  splitted.registerTempTable(\"splitted\")");
      printer.println("  splitted.sqlContext.sql(\"SELECT word, count(*) as count FROM splitted GROUP BY word\")");
      printer.println("}");
    }

    testWordCountCompute(codeWriter.toString());
  }

  @Test
  public void testScalaSparkComputeRDD() throws Exception {
    StringWriter codeWriter = new StringWriter();
    try (PrintWriter printer = new PrintWriter(codeWriter, true)) {
      printer.println(
        "def transform(rdd: RDD[StructuredRecord], context:SparkExecutionPluginContext) : RDD[StructuredRecord] = {");
      printer.println("  val schema = context.getOutputSchema");
      printer.println("  rdd");
      printer.println("    .flatMap(_.get[String](\"body\").split(\"\\\\s+\"))");
      printer.println("    .map(s => (s, 1L))");
      printer.println("    .reduceByKey(_ + _)");
      printer.println("    .map(t => StructuredRecord.builder(schema).set(\"word\", t._1).set(\"count\", t._2).build)");
      printer.println("}");
    }

    testWordCountCompute(codeWriter.toString());
  }

  private void testWordCountCompute(String code) throws Exception {
    Schema inputSchema = Schema.recordOf(
      "input",
      Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );

    Schema computeSchema = Schema.recordOf(
      "output",
      Schema.Field.of("word", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("count", Schema.nullableOf(Schema.of(Schema.Type.LONG)))
    );

    String inputTable = UUID.randomUUID().toString();
    String outputTable = UUID.randomUUID().toString();

    // Pipeline configuration
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(inputTable, inputSchema)))
      .addStage(new ETLStage("compute", new ETLPlugin("ScalaSparkCompute", SparkCompute.PLUGIN_TYPE, ImmutableMap.of(
        "scalaCode", code,
        "schema", computeSchema.toString()
      ))))
      .addStage(new ETLStage("sink", MockSink.getPlugin(outputTable)))
      .addConnection("source", "compute")
      .addConnection("compute", "sink")
      .build();

    // Deploy the pipeline
    ArtifactSummary artifactSummary = new ArtifactSummary(DATAPIPELINE_ARTIFACT_ID.getArtifact(),
                                                          DATAPIPELINE_ARTIFACT_ID.getVersion());
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(artifactSummary, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(UUID.randomUUID().toString());
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    // write records to source
    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset(inputTable));
    List<StructuredRecord> inputRecords = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      inputRecords.add(StructuredRecord.builder(inputSchema).set("body", "Line " + i).build());
    }
    MockSource.writeInput(inputManager, inputRecords);

    // Run the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // Verify result written to sink.
    // It has two fields, word and count.
    DataSetManager<Table> sinkManager = getDataset(outputTable);
    Map<String, StructuredRecord> wordCounts =
      Maps.uniqueIndex(Sets.newHashSet(MockSink.readOutput(sinkManager)), new Function<StructuredRecord, String>() {
        @Override
        public String apply(StructuredRecord record) {
          return record.get("word");
        }
      });

    Assert.assertEquals(11, wordCounts.size());
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(1L, wordCounts.get(Integer.toString(i)).get("count"));
    }
    Assert.assertEquals(10L, wordCounts.get("Line").get("count"));
  }

  @Test
  public void testScalaSparkSinkRDD() throws Exception {
    File testFolder = TEMP_FOLDER.newFolder("scalaSinkRDDOutput");
    File outputFolder = new File(testFolder, "output");
    StringWriter codeWriter = new StringWriter();
    try (PrintWriter printer = new PrintWriter(codeWriter, true)) {
      printer.println(
        "def sink(rdd: RDD[StructuredRecord], context:SparkExecutionPluginContext) : Unit = {");
      printer.println("  val schema = context.getOutputSchema");
      printer.println("  rdd");
      printer.println("    .flatMap(_.get[String](\"body\").split(\"\\\\s+\"))");
      printer.println("    .map(s => (s, 1L))");
      printer.println("    .reduceByKey(_ + _)");
      printer.println("    .map(t => t._1 + \" \" + t._2)");
      printer.println("    .saveAsTextFile(\"" + outputFolder.getAbsolutePath() + "\")");
      printer.println("}");
    }
    testWordCountSink(codeWriter.toString(), outputFolder);
  }

  @Test
  public void testScalaSparkSinkDataFrame() throws Exception {
    File testFolder = TEMP_FOLDER.newFolder("scalaSinkDataframeOutput");
    File outputFolder = new File(testFolder, "output");
    StringWriter codeWriter = new StringWriter();
    try (PrintWriter printer = new PrintWriter(codeWriter, true)) {
      printer.println("def sink(df: DataFrame) : Unit = {");
      printer.println("  val splitted = df.explode(\"body\", \"word\") { ");
      printer.println("    line: String => line.split(\"\\\\s+\")");
      printer.println("  }");
      printer.println("  splitted.registerTempTable(\"splitted\")");
      printer.println("  val query = \"SELECT CONCAT(word, ' ', count(*)) FROM splitted GROUP BY word\"");
      printer.println("  val out = splitted.sqlContext.sql(query)");
      printer.println("  out.write.format(\"text\").save(\"" + outputFolder.getAbsolutePath() + "\")");
      printer.println("}");
    }
    testWordCountSink(codeWriter.toString(), outputFolder);
  }

  private void testWordCountSink(String code, File outputFolder) throws Exception {
    Schema inputSchema = Schema.recordOf(
      "input",
      Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );

    String inputTable = UUID.randomUUID().toString();

    // Pipeline configuration
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(inputTable, inputSchema)))
      .addStage(new ETLStage("sink", new ETLPlugin("ScalaSparkSink", SparkSink.PLUGIN_TYPE,
                                                   ImmutableMap.of("scalaCode", code))))
      .addConnection("source", "sink")
      .build();

    // Deploy the pipeline
    ArtifactSummary artifactSummary = new ArtifactSummary(DATAPIPELINE_ARTIFACT_ID.getArtifact(),
                                                          DATAPIPELINE_ARTIFACT_ID.getVersion());
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(artifactSummary, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app(UUID.randomUUID().toString());
    ApplicationManager appManager = deployApplication(appId.toId(), appRequest);

    // write records to source
    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset(inputTable));
    List<StructuredRecord> inputRecords = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      inputRecords.add(StructuredRecord.builder(inputSchema).set("body", "Line " + i).build());
    }
    MockSource.writeInput(inputManager, inputRecords);

    // Run the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // Verify result written to sink.
    // It has two fields, word and count.
    Map<String, Long> wordCounts = new HashMap<>();
    for (File outputFile : outputFolder.listFiles()) {
      String fileName = outputFile.getName();
      if (fileName.startsWith(".") || "_SUCCESS".equals(fileName)) {
        continue;
      }
      try (BufferedReader reader = new BufferedReader(new FileReader(outputFile))) {
        String line;
        while ((line = reader.readLine()) != null) {
          String[] fields = line.split(" ");
          wordCounts.put(fields[0], Long.valueOf(fields[1]));
        }
      }
    }
    Assert.assertEquals(11, wordCounts.size());
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(1L, (long) wordCounts.get(String.valueOf(Integer.toString(i))));
    }
    Assert.assertEquals(10L, (long) wordCounts.get("Line"));
  }
}
