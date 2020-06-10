# Spark Sink in Scala

Description
-----------
Executes user-provided Spark code in Scala that operates on an input RDD or Dataframe with full
access to all Spark features.

Use Case
--------
This plugin can be used when you want to have complete control on the Spark computation.
For example, you may want to join the input RDD with another Dataset and select a subset
of the join result using Spark SQL before writing the results out to files in parquet format.

Properties
----------
**scalaCode** Spark code in Scala defining how to transform RDD to RDD. 
The code must implement a function called ``sink``, whose signature should be one of:

    def sink(df: DataFrame) : DataFrame

    def sink(df: DataFrame, context: SparkExecutionPluginContext) : DataFrame
    
The input ``DataFrame`` has the same schema as the input schema to this stage.
Using the ``SparkExecutionPluginContext``, you can access CDAP
entities such as Datasets, as well as providing access to the underlying ``SparkContext`` in use.
 
Operating on lower level ``RDD`` is also possible by using the one of the following forms of the ``sink`` method:

    def sink(rdd: RDD[StructuredRecord]) : RDD[StructuredRecord]

    def sink(rdd: RDD[StructuredRecord], context: SparkExecutionPluginContext) : RDD[StructuredRecord]
   
For example:

    def sink(rdd: RDD[StructuredRecord], context: SparkExecutionPluginContext) : Unit = {
      val outputSchema = context.getOutputSchema
      rdd
        .flatMap(_.get[String]("body").split("\\s+"))
        .map(s => (s, 1))
        .reduceByKey(_ + _)
        .saveAsTextFile("output")
    }
        
This will perform a word count on the input field ``'body'``, then write out the results as a text file.

The following imports are included automatically and are ready for the user code to use:

      import io.cdap.cdap.api.data.format._
      import io.cdap.cdap.api.data.schema._;
      import io.cdap.cdap.etl.api.batch._
      import org.apache.spark._
      import org.apache.spark.api.java._
      import org.apache.spark.rdd._
      import org.apache.spark.sql._
      import org.apache.spark.SparkContext._
      import scala.collection.JavaConversions._


**deployCompile** Specify whether the code will get validated during pipeline creation time. Setting this to `false`
will skip the validation.