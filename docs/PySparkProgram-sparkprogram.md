# Spark Program in Python

Description
-----------
Executes user-provided Spark code in Python.

Use Case
--------
This plugin can be used when you want to run arbitrary Spark code.

Properties
----------
* **pythonCode**
 
  The self-contained Spark application written in Python.
  For example, the [Naive Bayes Machine Learning](https://spark.apache.org/docs/1.6.3/mllib-naive-bayes.html) 
  from the official Spark documentation can be written as:

      # Import libraries
      from pyspark import *
      from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
      from pyspark.mllib.linalg import Vectors
      from pyspark.mllib.regression import LabeledPoint
      
      def parseLine(line):
          parts = line.split(',')
          label = float(parts[0])
          features = Vectors.dense([float(x) for x in parts[1].split(' ')])
          return LabeledPoint(label, features)
      
      sc = SparkContext()
      data = sc.textFile("${input.path}").map(parseLine)
      
      # Split data aproximately into training (60%) and test (40%)
      training, test = data.randomSplit([0.6, 0.4], seed=0)
      
      # Train a naive Bayes model.
      model = NaiveBayes.train(training, 1.0)
      
      # Make prediction and test accuracy.
      predictionAndLabel = test.map(lambda p: (model.predict(p.features), p.label))
      accuracy = 1.0 * predictionAndLabel.filter(lambda (x, v): x == v).count() / test.count()
      
      # Save and load model
      model.save(sc, "${output.path}")

  With the `input.path` and `output.path` being provided at runtime. 
 
* **pyFiles** 

  Extra libraries for the PySpark program. 
  It is a ',' separated list of URI for the locations of extra .egg, .zip and .py libraries.
