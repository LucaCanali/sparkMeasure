## Python

5. How to collect stage metrics, example in Python:
```python
stageMetrics = sc._jvm.ch.cern.sparkmeasure.StageMetrics(spark._jsparkSession)
stageMetrics.begin()
spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
stageMetrics.end()
stageMetrics.printReport()
stageMetrics.printAccumulables()
```

6. How to collect task metrics, example in Python: 
```python
taskMetrics = sc._jvm.ch.cern.sparkmeasure.TaskMetrics(spark._jsparkSession, False)
taskMetrics.begin()
spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
taskMetrics.end()
taskMetrics.printReport()
