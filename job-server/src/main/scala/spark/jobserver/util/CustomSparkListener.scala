package spark.jobserver.util

import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.{SparkListenerApplicationStart,
  SparkListenerApplicationEnd, SparkListenerExecutorAdded, SparkListenerExecutorRemoved}
import org.slf4j.LoggerFactory

class CustomSparkListener extends SparkListener{
  val logger = LoggerFactory.getLogger(getClass)

  private def applicationStartSparkListener = new SparkListener() {
      override def onApplicationStart(event: SparkListenerApplicationStart) {
        logger.info("[BEHROZ]==>Got Spark Application start event, starting job manger.")
      }
  }

  private def executorAddedSparkListener = new SparkListener() {
      override def onExecutorAdded(event: SparkListenerExecutorAdded) {
        logger.info("[BEHROZ]==>Got executor start event, added executor.")
      }
  }
}