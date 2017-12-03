package spark.jobserver.util

import org.slf4j.LoggerFactory
import org.apache.spark.launcher.SparkAppHandle

class SparkAppListener extends SparkAppHandle.Listener with Runnable {
    private val logger = LoggerFactory.getLogger("spark-app-listener")

    override def stateChanged(handle: SparkAppHandle) {
        val sparkAppId = handle.getAppId();
        val appState = handle.getState();
        logger.info("Spark job with app id: " + sparkAppId + ", State changed to: " + appState);
    }

     override def infoChanged(handle: SparkAppHandle) {
       val sparkAppId = handle.getAppId();
       val appState = handle.getState();
       logger.info("[INFO] ->Spark job with app id: " + sparkAppId + ", State changed to: " + appState);
     }

     override def run() {}
}