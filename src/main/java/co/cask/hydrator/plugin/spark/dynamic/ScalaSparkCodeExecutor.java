/*
 * Copyright © 2018 Cask Data, Inc.
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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.spark.dynamic.CompilationFailureException;
import co.cask.cdap.api.spark.dynamic.SparkInterpreter;
import co.cask.cdap.api.spark.sql.DataFrames;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkFirehoseListener;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.file.Files;
import javax.annotation.Nullable;

/**
 * Compiles and executes scala spark code that takes an RDD or Dataframe as input.
 */
public class ScalaSparkCodeExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(ScalaSparkCodeExecutor.class);
  private static final String CLASS_NAME_PREFIX = "co.cask.hydrator.plugin.spark.dynamic.generated.UserScalaSpark$";
  private static final Class<?> DATAFRAME_TYPE = getDataFrameType();
  private static final Class<?>[][] ACCEPTABLE_PARAMETER_TYPES = new Class<?>[][] {
    { RDD.class, SparkExecutionPluginContext.class },
    { RDD.class },
    { DATAFRAME_TYPE, SparkExecutionPluginContext.class},
    { DATAFRAME_TYPE }
  };

  private final String code;
  private final String dependencies;
  private final String methodName;
  private final boolean isVoid;
  private final ThreadLocal<SQLContext> sqlContextThreadLocal = new InheritableThreadLocal<>();

  // A strong reference is needed to keep the compiled classes around
  @SuppressWarnings("FieldCanBeLocal")
  private transient SparkInterpreter interpreter;
  private transient Method method;
  private transient boolean isDataFrame;
  private transient boolean takeContext;

  public ScalaSparkCodeExecutor(String code, @Nullable String dependencies, String methodName, boolean isVoid) {
    this.code = code;
    this.dependencies = dependencies;
    this.methodName = methodName;
    this.isVoid = isVoid;
  }

  /**
   * Runs validation to be performed at configure time.
   */
  public void configure(@Nullable Schema inputSchema) {

    SparkInterpreter interpreter = SparkCompilers.createInterpreter();
    if (interpreter != null) {
      File dir = null;
      try {
        if (dependencies != null) {
          dir = Files.createTempDirectory("sparkprogram").toFile();
          SparkCompilers.addDependencies(dir, interpreter, dependencies);
        }
        // We don't need the actual stage name as this only happen in deployment time for compilation check.
        String className = generateClassName("dummy");
        interpreter.compile(generateSourceClass(className));

        // Make sure it has a valid transform method
        Method method = getMethod(interpreter.getClassLoader(), className);

        // If the method takes DataFrame, make sure it has input schema
        if (isDataFrame(method.getParameterTypes()[0]) && inputSchema == null) {
          throw new IllegalArgumentException("Missing input schema for transformation using DataFrame");
        }

      } catch (CompilationFailureException e) {
        throw new IllegalArgumentException(e.getMessage(), e);
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        SparkCompilers.deleteDir(dir);
      }
    }
  }

  /**
   * Initializes the interpreter
   */
  public void initialize(SparkExecutionPluginContext context) throws Exception {
    String className = generateClassName(context.getStageName());
    interpreter = context.createSparkInterpreter();
    File tempDir = null;
    if (dependencies != null) {
      tempDir = Files.createTempDirectory("sparkprogram").toFile();
      SparkCompilers.addDependencies(tempDir, interpreter, dependencies);
    }
    // Release resources on application completion.
    final File finalTempDir = tempDir;
    SparkFirehoseListener sparkListener = new SparkFirehoseListener() {
      @Override
      public void onEvent(SparkListenerEvent event) {
        if (event instanceof SparkListenerApplicationEnd) {
          LOG.info("Releasing resources on Spark application completion.");
          interpreter.close();
          if (finalTempDir != null) {
            SparkCompilers.deleteDir(finalTempDir);
          }
        }
      }
    };
    // Need to use reflection to find and call the addSparkListener() method, due to incompatible changes
    // between Spark1 (SparkListener) and Spark2 (SparkListenerInterface).
    SparkContext sc = context.getSparkContext().sc();
    for (Method method : sc.getClass().getMethods()) {
      if (method.getName().equals("addSparkListener")) {
        Class<?>[] paramTypes = method.getParameterTypes();
        if (paramTypes.length == 1 && paramTypes[0].isAssignableFrom(sparkListener.getClass())) {
          method.invoke(sc, sparkListener);
          break;
        }
      }
    }

    interpreter.compile(generateSourceClass(className));
    method = getMethod(interpreter.getClassLoader(), className);

    isDataFrame = isDataFrame(method.getParameterTypes()[0]);
    takeContext = method.getParameterTypes().length == 2;

    // Input schema shouldn't be null
    if (isDataFrame && context.getInputSchema() == null) {
      throw new IllegalArgumentException("Input schema must be provided for using DataFrame in Spark Compute");
    }
  }

  /**
   * Execute interpreted code on the given RDD.
   */
  public Object execute(SparkExecutionPluginContext context,
                        JavaRDD<StructuredRecord> javaRDD) throws InvocationTargetException, IllegalAccessException {
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
    Schema inputSchema = context.getInputSchema();
    if (inputSchema == null) {
      // Should already been checked in initialize. This is to safeguard in case the call sequence changed in future.
      throw new IllegalArgumentException("Input schema must be provided for using DataFrame in Spark Compute");
    }

    SQLContext sqlContext = getSQLContext(context.getSparkContext().sc());

    StructType rowType = DataFrames.toDataType(inputSchema);
    JavaRDD<Row> rowRDD = javaRDD.map(new RecordToRow(rowType));

    Object dataFrame = createDataFrame(sqlContext, rowRDD, rowType);
    return takeContext ? method.invoke(null, dataFrame, context) : method.invoke(null, dataFrame);
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
      sourceWriter.println(code);
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

  private Method getMethod(ClassLoader classLoader, String className) {
    // Use reflection to load the class and get the transform method
    try {
      Class<?> computeClass = classLoader.loadClass(className);

      // Find which method to call
      Method method = null;
      for (Class<?>[] paramTypes : ACCEPTABLE_PARAMETER_TYPES) {
        method = tryFindMethod(computeClass, methodName, paramTypes);
        if (method != null) {
          break;
        }
      }

      String returnType1 = isVoid ? "Unit" : "RDD[StructuredRecord]";
      String returnType2 = isVoid ? "Unit" : "DataFrame[StructuredRecord]";
      if (method == null) {
        throw new IllegalArgumentException(String.format(
          "Missing a `%s` method that has signature in one of the following form\n" +
            "def %s(rdd: RDD[StructuredRecord]) : %s\n" +
            "def %s(rdd: RDD[StructuredRecord], context: SparkExecutionPluginContext) : %s\n" +
            "def %s(dataframe: DataFrame) : %s\n" +
            "def %s(dataframe: DataFrame, context: SparkExecutionPluginContext) : %s",
          methodName, methodName, returnType1, methodName, returnType1, methodName, returnType2,
          methodName, returnType2));
      }

      Type[] parameterTypes = method.getGenericParameterTypes();
      boolean isDataFrame = isDataFrame(parameterTypes[0]);

      // The first parameter should be of type RDD[StructuredRecord] if it takes RDD
      if (!isDataFrame) {
        validateRDDType(parameterTypes[0], String.format(
          "The first parameter of the '%s' method should have type as 'RDD[StructuredRecord]'", methodName));
      }

      // If it has second parameter, then must be SparkExecutionPluginContext
      if (parameterTypes.length == 2 && !SparkExecutionPluginContext.class.equals(parameterTypes[1])) {
        throw new IllegalArgumentException(String.format(
          "The second parameter of the '%s' method should have type as SparkExecutionPluginContext", methodName));
      }

      // The return type of the method must be Unit
      if (isVoid) {
        if (!void.class.equals(method.getReturnType())) {
          throw new IllegalArgumentException(String.format("The return type of the '%s' method should be 'Unit'",
                                                           methodName));
        }
      } else {
        // The return type of the method must be RDD[StructuredRecord] if it takes RDD
        // Or it must be DataFrame if it takes DataFrame
        if (isDataFrame) {
          if (!isDataFrame(method.getGenericReturnType())) {
            throw new IllegalArgumentException(String.format(
              "The return type of the '%s' method should be 'DataFrame'", methodName));
          }
        } else {
          validateRDDType(method.getGenericReturnType(), String.format(
            "The return type of the '%s' method should be 'RDD[StructuredRecord]'", methodName));
        }
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
   * Returns whether the given {@link Type} is a DataFrame type.
   */
  private boolean isDataFrame(Type type) {
    if (DATAFRAME_TYPE == null) {
      return false;
    }
    if (type instanceof Class) {
      return ((Class<?>) type).isAssignableFrom(DATAFRAME_TYPE);
    }
    if (type instanceof ParameterizedType) {
      return isDataFrame(((ParameterizedType) type).getRawType());
    }
    return false;
  }

  private Object createDataFrame(SQLContext sqlContext, JavaRDD<Row> rdd, StructType type) {
    try {
      return sqlContext.getClass()
        .getMethod("createDataFrame", JavaRDD.class, StructType.class)
        .invoke(sqlContext, rdd, type);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Unable to find SQLContext.createDataFrame(JavaRDD, StructType) method", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("No permission to invoke SQLContext.createDataFrame(JavaRDD, StructType) method", e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException("Failed to invoke SQLContext.createDataFrame(JavaRDD, StructType) method", e);
    }
  }

  @Nullable
  private static Class<?> getDataFrameType() {
    // For Spark1, it has the DataFrame class
    // For Spark2, there is no more DataFrame class, and it becomes Dataset<Row>
    try {
      return ScalaSparkSink.class.getClassLoader().loadClass("org.apache.spark.sql.DataFrame");
    } catch (ClassNotFoundException e) {
      try {
        return ScalaSparkSink.class.getClassLoader().loadClass("org.apache.spark.sql.Dataset");
      } catch (ClassNotFoundException e1) {
        LOG.warn("Failed to determine the type of Spark DataFrame. " +
                   "DataFrame is not supported in the ScalaSparkCompute plugin.");
        return null;
      }
    }
  }
}
