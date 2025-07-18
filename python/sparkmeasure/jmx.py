from typing import Union, Dict

import logging
from sparkmeasure import StageMetrics
import datetime

logger = logging.getLogger(__name__)

def jmxexport(spark_session, metrics: Dict[str, Union[float, int]]):
    """Publish metrics to Dropwizard via JMX."""
    try:
        publish_metrics_count = 0
        dropwizard = spark_session._jvm.ch.cern.metrics.DropwizardMetrics
        for key, value in metrics.items():
            # or setGauge
            is_counter = True
            dropwizard.setMetricAutoType(key, float(value))
            # Example counter to track the number of times metrics have been published
            publish_metrics_count += 1

        dropwizard.setMetricAutoType("metrics_published_total", float(publish_metrics_count))

        logger.info("%d Dropwizard metrics published via JMX", len(metrics))
    except Exception as e:
        logger.error("Failed to publish Dropwizard metrics: %s", e)
