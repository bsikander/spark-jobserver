package spark.jobserver.util

import org.slf4j.LoggerFactory
import org.apache.spark.scheduler._

class ManagerSparkListener extends SparkListener {
  private val logger = LoggerFactory.getLogger("manager-app-listener")

  override def onApplicationStart(arg0: SparkListenerApplicationStart) {
     logger.info( "[LISTENER] Application Start attemptId: " + arg0.appAttemptId +
         " appId: " + arg0.appId + " appName: " + arg0.appName)
  }

  override def onApplicationEnd(arg0: SparkListenerApplicationEnd) {
     logger.info("[LISTENER] Application End");
  }

  override def onExecutorAdded(arg0: SparkListenerExecutorAdded) {
    logger.info("[LISTENER] Executor Added executorId: " + arg0.executorId +
         " cores: " + arg0.executorInfo.totalCores + " executorHost: " + arg0.executorInfo.executorHost)
  }

  override def onExecutorRemoved(arg0: SparkListenerExecutorRemoved) {
    logger.info("[LISTENER] Executor End")
  }

  override def  onJobStart(arg0 : SparkListenerJobStart) {
    logger.info("[LISTENER] Job Start")
  }

  override def  onJobEnd(arg0 : SparkListenerJobEnd) {
    logger.info("[LISTENER] Job End")
  }

}