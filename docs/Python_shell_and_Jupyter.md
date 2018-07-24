## Python

### Stage metrics
1. How to collect stage metrics, example in Python:
```python
from sparkmeasure import StageMetrics
stagemetrics = StageMetrics(spark)

stagemetrics.begin()
spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
stagemetrics.end()
stagemetrics.print_report()
```

2. Sae as example 1, but with a shortcut/workaround to run code and measure it with one command line:
 ```python
 from sparkmeasure import StageMetrics
 stagemetrics = StageMetrics(spark)

stagemetrics.runandmeasure(locals(),
  'spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()')
 ```

---
### Task metrics
2. How to collect task metrics, example in Python: 
```python
taskMetrics = sc._jvm.ch.cern.sparkmeasure.TaskMetrics(spark._jsparkSession, False)
taskMetrics.begin()
spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()
taskMetrics.end()
taskMetrics.printReport()
