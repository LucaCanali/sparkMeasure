# testSparkMeasureScala

This is example code of how you can use sparkMeasure to instrument your Scala code
running Apache Spark workloads.  
More info at [https://github.com/LucaCanali/sparkMeasure]

How to run the example:
```
# build the example jar
sbt package

bin/spark-submit --master local[*] --packages ch.cern.sparkmeasure:spark-measure_2.12:0.23 --class ch.cern.testSparkMeasure.testSparkMeasure <path_to_the_example_jar>/testsparkmeasurescala_2.12-0.1.jar
```
