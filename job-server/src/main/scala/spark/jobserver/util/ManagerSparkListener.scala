package spark.jobserver.util

import org.slf4j.LoggerFactory
import org.apache.spark.scheduler._
import akka.actor.{ActorRef, PoisonPill}
import spark.jobserver.JobManagerActor.{ExecutorAdded, ExecutorRemoved}

class ManagerSparkListener(contextName: String, actorRef: ActorRef) extends SparkListener {
  private val logger = LoggerFactory.getLogger("manager-app-listener")

  override def onApplicationEnd(arg0: SparkListenerApplicationEnd) {
     logger.info("Got Spark Application end event, stopping job manager.")
     actorRef ! PoisonPill
  }

  override def onExecutorAdded(executor: SparkListenerExecutorAdded) {
    logger.info("Executor Added context name: " + contextName +
	       " executorId: " + executor.executorId +
         " cores: " + executor.executorInfo.totalCores +
         " executorHost: " + executor.executorInfo.executorHost)
    actorRef ! ExecutorAdded
    logger.info("ExecutorAdded message sent to the Driver")
  }

  override def onExecutorRemoved(executor: SparkListenerExecutorRemoved) {
    logger.info("Executor removed for context name: " + contextName +
         " executorId: " + executor.executorId +
         " reason: " + executor.reason +
         " time: " + executor.time)
    actorRef ! ExecutorRemoved
    logger.info("ExecutorRemoved message sent to the Driver")
  }
}