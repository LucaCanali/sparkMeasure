package ch.cern.metrics

import java.io.File
import org.slf4j.LoggerFactory
import scala.io.Source

import com.codahale.metrics.{Counter, Gauge, MetricRegistry, Timer}
import com.codahale.metrics.jmx.JmxReporter

object DropwizardMetrics {
  val registry = new MetricRegistry()
  private val logger = LoggerFactory.getLogger(getClass)

  private def getNamespace(): String = {
    val path = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
    val file = new File(path)
    if (file.exists && file.canRead)
      Source.fromFile(file).getLines().mkString.trim
    else "unknown"
  }

  private def getPodName(): String = {
    sys.env.getOrElse("HOSTNAME", "unknown")
  }

  // Simple cache local pour éviter double registration
  private val knownGauges = scala.collection.mutable.Set[String]()
  private val knownCounters = scala.collection.mutable.Set[String]()
  private val knownTimers = scala.collection.mutable.Set[String]()

  // Démarre le JMX reporter une seule fois
  private val reporter: JmxReporter = JmxReporter
    .forRegistry(registry)
    .inDomain("sparkmeasure.metrics") // <== domaine JMX
    .build()

  reporter.start()

def setMetricAutoType(shortname: String, value: Double): Unit = {
  val lower = shortname.toLowerCase

  val metricType =
    if (lower.contains("time") || lower.contains("duration")) "timer"
    else if (lower.contains("peak") || lower.contains("size")) "gauge"
    else "counter"

  setMetric(shortname, value, metricType)
}

def setMetric(shortname: String, value: Double, metricType: String): Unit = {
  val kind = metricType.toLowerCase
  val name = s"${getNamespace()}.${getPodName()}.$shortname"

  logger.debug(s"[JMX] Setting $kind: $shortname = $value")

  kind match {
    case "counter" =>
      if (!knownCounters.contains(name)) {
        counters(name) = registry.counter(name)
        knownCounters.add(name)
      }
      val current = counters(name).getCount
      val delta = value.toLong - current
      if (delta > 0) {
        counters(name).inc(delta)
      } // sinon on ne décrémente pas un counter

    case "gauge" =>
      if (!knownGauges.contains(name)) {
        registry.register(name, new Gauge[Double] {
          override def getValue: Double = gauges.getOrElse(name, 0.0)
        })
        knownGauges.add(name)
      }
      gauges.update(name, value)

    case "timer" =>
      if (!knownTimers.contains(name)) {
        timers(name) = registry.timer(name)
        knownTimers.add(name)
      }
      // on suppose que value est en millisecondes
      timers(name).update(value.toLong, java.util.concurrent.TimeUnit.MILLISECONDS)

    case _ =>
      throw new IllegalArgumentException(s"Unknown metric type: $metricType")
  }
}

def describeMetrics() : String = {
  val sb = new StringBuilder
  sb.append(s"Metrics for namespace '${getNamespace()}' and pod '${getPodName()}'\n")
  sb.append(s"Total gauges: ${gauges.size}\n")
  gauges.foreach { case (name, value) =>
    sb.append(s"Gauge: $name = $value\n")
  }
  sb.append(s"Total counters: ${counters.size}\n")
  counters.foreach { case (name, counter) =>
    sb.append(s"Counter: $name = ${counter.getCount}\n")
  }
  sb.append(s"Total timers: ${timers.size}\n")
  timers.foreach { case (name, timer) =>
    val snapshot = timer.getSnapshot
    sb.append(s"Timer: $name = " +
      s"count=${timer.getCount}\n")
  }
  sb.toString()
}

  private val gauges = scala.collection.concurrent.TrieMap[String, Double]()
  private val counters = scala.collection.concurrent.TrieMap[String, Counter]()
  private val timers = scala.collection.concurrent.TrieMap[String, Timer]()
}
