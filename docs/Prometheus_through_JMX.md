## Exporting sparkMeasure Metrics to Prometheus via JMX

`sparkMeasure` collects execution metrics from Spark jobs at the driver or executor level. While it does not expose its metrics directly via JMX, it can be used alongside Spark's JMX metrics system to enable Prometheus-based monitoring.

In a Kubernetes environment using the **Spark Operator**, you can configure the Spark driver and executor to expose their sparkMeasure metrics through JMX Prometheus exporter and scrape them with Prometheus.

> ✅ This setup has been validated **only** in **Kubernetes environments using the [Spark Operator](https://www.kubeflow.org/docs/components/spark-operator)**.

### Enable the JMX prometheus exporter in Spark

To configure JMX and Prometheus exporter monitoring with Spark on Kubernetes, follow the official Kubeflow Spark Operator documentation:

📖 [Monitoring with JMX and Prometheus — Kubeflow Spark Operator Guide](https://www.kubeflow.org/docs/components/spark-operator/user-guide/monitoring-with-jmx-and-prometheus/)

### Exporting `sparkMeasure` Metrics via JMX in Python

To programmatically export `sparkMeasure` metrics in Python alongside standard JMX metrics, you can leverage the `jmxexport` function from the `sparkmeasure.jmx` module. This enables custom metrics collected during job execution to be exposed through the same Prometheus exporter as native Spark metrics.

#### Example Usage

```python
from sparkmeasure import StageMetrics
from sparkmeasure.jmx import jmxexport

stage_metrics = StageMetrics(spark)
stage_metrics.begin()

# ... run your Spark jobs here ...

stage_metrics.end()
current_metrics = stagemetrics.aggregate_stagemetrics()

# export the metrics to JMX Prometheus exporter
jmxexport(spark, current_metrics)
```

 The `jmxexport()` call updates the current Spark application’s JMX metrics with the `sparkMeasure` results, making them available to any configured Prometheus instance.

See a full implementation example here:
📄 [How to use the JMX exporter in Python code](../e2e/rootfs/opt/spark/examples/spark-sql.py)

---

### Prometheus Exporter Configuration

In addition to exposing metrics via JMX, you must configure the Prometheus JMX exporter in the Spark driver and executor pods to make the custom `sparkMeasure` metrics queryable by Prometheus. This configuration should be added *on top of* the existing JMX metrics exporter configuration.

Ensure your Spark pod manifest or Helm chart includes a properly configured `ConfigMap` for the JMX exporter. Specifically, you’ll need to add mappings for the custom `sparkMeasure` metrics to the YAML under the `rules` section used by the Prometheus JMX exporter.

A production-ready configuration example is available here:
📄 [How to configure the Prometheus exporter to expose sparkMeasure metrics](../e2e/charts/spark-jobs/templates/jmx-configmap.yaml)

---

By combining Python-based metric collection with a Prometheus-compatible JMX exporter, you can ensure comprehensive observability for Spark applications, including custom performance instrumentation through `sparkMeasure`.

> **Security Tip:** In production environments, ensure that JMX ports are protected using appropriate Kubernetes NetworkPolicies or service mesh configurations. Avoid exposing unauthenticated JMX endpoints externally to mitigate the risk of unauthorized access.
