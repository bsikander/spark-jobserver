package spark.jobserver.util

import scala.util.Try
import scala.sys.process.{Process, ProcessLogger}
import java.io.File
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.launcher.SparkAppHandle

/**
 * This class aims to eliminate the need to call spark-submit
 * through scripts. Currently, spark-submit is used in 2 files
 * manager_start.sh and server_start.sh. Both of these scripts
 * need some variables which are set inside setenv.sh script.
 *
 * When server_start.sh is executed it sources the setenv.sh
 * script. set -a flag enables exporting the variables to
 * environment. Launcher uses those environment variables to
 * start context JVMs using SparkLauncher class.
 */
abstract class Launcher(config: Config) {
    private val logger = LoggerFactory.getLogger("spark-launcher")

    protected final val master = config.getString("spark.master")
    protected final val deployMode = config.getString("spark.submit.deployMode")

    protected final val currentWorkingDirectory = getEnvironmentVariable("appdir")
    logger.info(s"Spark launcher working directory is $currentWorkingDirectory")

    protected final val sjsJarPath = getEnvironmentVariable("MANAGER_JAR_FILE")
    protected final val baseGCOPTS = getEnvironmentVariable("GC_OPTS_BASE")
    protected final val baseJavaOPTS = getEnvironmentVariable("JAVA_OPTS_BASE")
    private var handler: SparkAppHandle = null
    protected val launcher = new SparkLauncher()
    initSparkLauncher()

    protected def addCustomArguments()

    final def start(): Boolean = {
      if (!validate()) return false

      logger.info("Add custom arguments to launcher")
      addCustomArguments()

      logger.info("Start launcher application")
      handler = launcher.startApplication()
      logger.info(s"Current state of application $getState")
      true
    }

    final def getState(): String = {
      handler.getState().name()
    }

    protected final def getEnvironmentVariable(name: String): String = {
      sys.env.get(name).getOrElse("")
    }

    private def initSparkLauncher() {
      logger.info("Initializing spark launcher")
      launcher.setSparkHome(getEnvironmentVariable("SPARK_HOME"))
      launcher.setMaster(master)
      launcher.setDeployMode(deployMode)
      launcher.setAppResource(sjsJarPath)
    }

    private def validate(): Boolean = {
      if (currentWorkingDirectory.isEmpty()) {
        logger.error("appdir environment variable is empty. Probably setenv.sh was not loaded properly.")
        false
      }
      true
    }
}
