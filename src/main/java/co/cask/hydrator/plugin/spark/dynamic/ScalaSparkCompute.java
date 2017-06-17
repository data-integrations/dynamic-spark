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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.spark.dynamic.CompilationFailureException;
import co.cask.cdap.api.spark.dynamic.SparkInterpreter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import javax.annotation.Nullable;

/**
 * A {@link SparkCompute} that takes any scala code and executes it.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("ScalaSparkCompute")
@Description("Executes user-provided Spark code written in Scala that performs RDD to RDD transformation")
public class ScalaSparkCompute extends SparkCompute<StructuredRecord, StructuredRecord> {

  private static final String CLASS_NAME_PREFIX = "co.cask.hydrator.plugin.spark.dynamic.generated.UserSparkCompute$";
  private static final Class<?>[][] ACCEPTABLE_PARAMETER_TYPES = new Class<?>[][] {
    { RDD.class, SparkExecutionPluginContext.class },
    { RDD.class },
    { DataFrame.class, SparkExecutionPluginContext.class},
    { DataFrame.class }
  };

  private final ThreadLocal<SQLContext> sqlContextThreadLocal = new InheritableThreadLocal<SQLContext>();

  private final transient Config config;
  // A strong reference is needed to keep the compiled classes around
  @SuppressWarnings("FieldCanBeLocal")
  private transient SparkInterpreter interpreter;
  private transient Method method;
  private transient boolean isDataFrame;
  private transient boolean takeContext;

  public ScalaSparkCompute(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    try {
      if (!config.containsMacro("schema")) {
        stageConfigurer.setOutputSchema(
          config.getSchema() == null ? stageConfigurer.getInputSchema() : Schema.parseJson(config.getSchema())
        );
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse output schema " + config.getSchema(), e);
    }

    if (!config.containsMacro("scalaCode")) {
      SparkInterpreter interpreter = SparkCompilers.createInterpreter();
      if (interpreter != null) {
        try {
          // We don't need the actual stage name as this only happen in deployment time for compilation check.
          String className = generateClassName("dummy");
          interpreter.compile(generateSourceClass(className));

          // Make sure it has a valid transform method
          Method method = getTransformMethod(interpreter.getClassLoader(), className);

          // If the method takes DataFrame, make sure it has input schema
          if (method.getParameterTypes()[0].equals(DataFrame.class) && stageConfigurer.getInputSchema() == null) {
            throw new IllegalArgumentException("Missing input schema for transformation using DataFrame");
          }

        } catch (CompilationFailureException e) {
          throw new IllegalArgumentException(e.getMessage(), e);
        }
      }
    }
  }

  @Override
  public void initialize(SparkExecutionPluginContext context) throws Exception {
    String className = generateClassName(context.getStageName());
    interpreter = context.createSparkInterpreter();
    interpreter.compile(generateSourceClass(className));
    method = getTransformMethod(interpreter.getClassLoader(), className);
    isDataFrame = method.getParameterTypes()[0].equals(DataFrame.class);
    takeContext = method.getParameterTypes().length == 2;
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context,
                                             JavaRDD<StructuredRecord> javaRDD) throws Exception {
    // RDD case
    if (!isDataFrame) {
      if (takeContext) {
        //noinspection unchecked
        return ((RDD<StructuredRecord>) method.invoke(null, javaRDD.rdd(), context)).toJavaRDD();
      } else {
        //noinspection unchecked
        return ((RDD<StructuredRecord>) method.invoke(null, javaRDD.rdd())).toJavaRDD();
      }
    }

    // DataFrame case
    SQLContext sqlContext = getSQLContext(context.getSparkContext().sc());

    StructType rowType = SparkDataFrames.toStructType(context.getInputSchema());
    JavaRDD<Row> rowRDD = javaRDD.map(new RecordToRow(rowType));

    DataFrame dataFrame = sqlContext.createDataFrame(rowRDD, rowType);
    DataFrame result = (DataFrame) (takeContext ?
      method.invoke(null, dataFrame, context) : method.invoke(null, dataFrame));

    // Convert the DataFrame back to RDD<StructureRecord>
    Schema outputSchema = context.getOutputSchema();
    if (outputSchema == null) {
      // If there is no output schema configured, derive it from the DataFrame
      // Otherwise, assume the DataFrame has the correct schema already
      outputSchema = SparkDataFrames.toSchema(result.schema());
    }
    return result.toJavaRDD().map(new RowToRecord(outputSchema));
  }

  private String generateSourceClass(String className) {
    StringWriter writer = new StringWriter();

    try (PrintWriter sourceWriter = new PrintWriter(writer, false)) {
      sourceWriter.println("package " + className.substring(0, className.lastIndexOf('.')));
      // Includes some commonly used imports.
      sourceWriter.println("import co.cask.cdap.api.data.format._");
      sourceWriter.println("import co.cask.cdap.api.data.schema._");
      sourceWriter.println("import co.cask.cdap.etl.api.batch._");
      sourceWriter.println("import org.apache.spark._");
      sourceWriter.println("import org.apache.spark.api.java._");
      sourceWriter.println("import org.apache.spark.rdd._");
      sourceWriter.println("import org.apache.spark.sql._");
      sourceWriter.println("import org.apache.spark.SparkContext._");
      sourceWriter.println("import scala.collection.JavaConversions._");
      sourceWriter.println("object " + className.substring(className.lastIndexOf('.') + 1) + " {");
      sourceWriter.println(config.getScalaCode());
      sourceWriter.println("}");
    }

    return writer.toString();
  }

  /**
   * Validates the given type is {@code RDD<StructuredRecord>}.
   */
  private void validateRDDType(Type rddType, String errorMessage) {
    if (!(rddType instanceof ParameterizedType)) {
      throw new IllegalArgumentException(errorMessage);
    }
    if (!RDD.class.equals(((ParameterizedType) rddType).getRawType())) {
      throw new IllegalArgumentException(errorMessage);
    }

    Type[] typeParams = ((ParameterizedType) rddType).getActualTypeArguments();
    if (typeParams.length < 1 || !typeParams[0].equals(StructuredRecord.class)) {
      throw new IllegalArgumentException(errorMessage);
    }
  }

  private SQLContext getSQLContext(SparkContext sc) {
    SQLContext sqlContext = sqlContextThreadLocal.get();
    if (sqlContext != null && !sqlContext.sparkContext().isStopped()) {
      return sqlContext;
    }

    synchronized (this) {
      sqlContext = sqlContextThreadLocal.get();
      if (sqlContext == null || sqlContext.sparkContext().isStopped()) {
        sqlContext = new SQLContext(sc);
        sqlContextThreadLocal.set(sqlContext);
      }
    }

    return sqlContext;
  }

  private Method getTransformMethod(ClassLoader classLoader, String className) {
    // Use reflection to load the class and get the transform method
    try {
      Class<?> computeClass = classLoader.loadClass(className);

      // Find which method to call
      Method method = null;
      for (Class<?>[] paramTypes : ACCEPTABLE_PARAMETER_TYPES) {
        method = tryFindMethod(computeClass, "transform", paramTypes);
        if (method != null) {
          break;
        }
      }

      if (method == null) {
        throw new IllegalArgumentException(
          "Missing a `transform` method that has signature in one of the following form\n" +
          "def transform(rdd: RDD[StructuredRecord]) : RDD[StructuredRecord]\n" +
          "def transform(rdd: RDD[StructuredRecord], context: SparkExecutionPluginContext) : RDD[StructuredRecord]\n" +
          "def transform(dataframe: DataFrame) : DataFrame\n" +
          "def transform(dataframe: DataFrame, context: SparkExecutionPluginContext) : DataFrame");
      }

      Type[] parameterTypes = method.getGenericParameterTypes();

      // The first parameter should be of type RDD[StructuredRecord] if it takes RDD
      if (!parameterTypes[0].equals(DataFrame.class)) {
        validateRDDType(parameterTypes[0],
                        "The first parameter of the 'transform' method should have type as 'RDD[StructuredRecord]'");
      }

      // If it has second parameter, then must be SparkExecutionPluginContext
      if (parameterTypes.length == 2 && !SparkExecutionPluginContext.class.equals(parameterTypes[1])) {
        throw new IllegalArgumentException(
          "The second parameter of the 'transform' method should have type as SparkExecutionPluginContext");
      }

      // The return type of the method must be RDD[StructuredRecord] if it takes RDD
      // Or it must be DataFrame if it takes DataFrame
      if (parameterTypes[0].equals(DataFrame.class)) {
        if (!method.getReturnType().equals(DataFrame.class)) {
          throw new IllegalArgumentException("The return type of the 'transform' method should be 'DataFrame'");
        }
      } else {
        validateRDDType(method.getGenericReturnType(),
                        "The return type of the 'transform' method should be 'RDD[StructuredRecord]'");
      }

      method.setAccessible(true);
      return method;
    } catch (ClassNotFoundException e) {
      // This shouldn't happen since we define the class name.
      throw new IllegalArgumentException(e);
    }
  }

  @Nullable
  private Method tryFindMethod(Class<?> cls, String name, Class<?>...parameterTypes) {
    try {
      return cls.getDeclaredMethod(name, parameterTypes);
    } catch (NoSuchMethodException e) {
      return null;
    }
  }

  private String generateClassName(String stageName) {
    // Hex encode any non-alphanumeric character in the stage name
    StringBuilder nameBuilder = new StringBuilder(CLASS_NAME_PREFIX);
    for (char c : stageName.toCharArray()) {
      if (Character.isLetter(c) || Character.isDigit(c)) {
        nameBuilder.append(c);
      } else {
        nameBuilder.append(String.format("%02X", (int) c));
      }
    }
    return nameBuilder.toString();
  }

  /**
   * Configuration object for the plugin
   */
  public static final class Config extends PluginConfig {

    @Description("Spark code in Scala defining how to transform RDD to RDD. " +
      "The code must implement a function " +
      "called 'transform', which has signature as either \n" +
      "  def transform(rdd: RDD[StructuredRecord]) : RDD[StructuredRecord]\n" +
      "  or\n" +
      "  def transform(rdd: RDD[StructuredRecord], context: SparkExecutionPluginContext) : RDD[StructuredRecord]\n" +
      "For example:\n" +
      "'def transform(rdd: RDD[StructuredRecord]) : RDD[StructuredRecord] = {\n" +
      "   rdd.filter(_.get(\"gender\") == null)\n" +
      " }'\n" +
      "will filter out incoming records that does not have the 'gender' field."
    )
    @Macro
    private final String scalaCode;

    @Description("The schema of output objects. If no schema is given, it is assumed that the output schema is " +
      "the same as the input schema.")
    @Nullable
    @Macro
    private final String schema;

    public Config(String scalaCode, @Nullable String schema) {
      this.scalaCode = scalaCode;
      this.schema = schema;
    }

    public String getScalaCode() {
      return scalaCode;
    }

    @Nullable
    public String getSchema() {
      return schema;
    }
  }

  /**
   * Function to map from {@link StructuredRecord} to {@link Row}.
   */
  public static final class RecordToRow implements Function<StructuredRecord, Row> {

    private final StructType rowType;

    public RecordToRow(StructType rowType) {
      this.rowType = rowType;
    }

    @Override
    public Row call(StructuredRecord record) throws Exception {
      return SparkDataFrames.toRow(record, rowType);
    }
  }

  /**
   * Function to map from {@link Row} to {@link StructuredRecord}.
   */
  public static final class RowToRecord implements Function<Row, StructuredRecord> {

    private final Schema schema;

    public RowToRecord(Schema schema) {
      this.schema = schema;
    }

    @Override
    public StructuredRecord call(Row row) throws Exception {
      return SparkDataFrames.fromRow(row, schema);
    }
  }
}
