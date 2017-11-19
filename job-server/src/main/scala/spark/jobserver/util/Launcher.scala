package spark.jobserver.util

import scala.util.Try
import com.typesafe.config.Config
import java.nio.file.{Files, Paths}
import org.slf4j.LoggerFactory
import scala.sys.process.{Process, ProcessLogger}
import org.apache.spark.launcher.SparkLauncher

abstract class Launcher(config: Config) {
    protected val master = Try(config.getString("spark.master")).toOption.getOrElse("local[4]")
    protected val deployMode = Try(config.getString("spark.submit.deployMode")).toOption.getOrElse("client")
    protected val setEnvScriptPath = config.getString("deploy.manager-start-cmd") //TODO:
    protected val sjsJarPath = Try(config.getString("deploy.sjs-jar-path")).toOption.getOrElse(setEnvScriptPath)
    protected val BASE_GC_OPTS = "-XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCTimeStamps -XX:MaxPermSize=512m -XX:+CMSClassUnloadingEnabled "

    protected val launcher = new SparkLauncher()
    this.initSparkLauncher()
    
    private def initSparkLauncher() {
      launcher.setSparkHome(this.getEnvironmentVariable("SPARK_HOME"))
      launcher.setMaster(master)
      launcher.setDeployMode(deployMode)
      launcher.setAppResource(sjsJarPath)
    }
  
    private def loadSetEnvScript(): Boolean = {
      val logger = LoggerFactory.getLogger("spark-launcher")
      val processLogger = ProcessLogger(out => logger.info(out), err => logger.warn(err))
  
      val setEnvScriptAbsolutePath = new java.io.File(setEnvScriptPath).getCanonicalPath()
      val setEnvExitCode = Process(setEnvScriptAbsolutePath).run(processLogger).exitValue()
      if (setEnvExitCode != 0) {
        logger.error(s"Failed to load environment variables from setenv.sh script, path $setEnvScriptPath")
        false
      } 
      true
    }

    protected def addCustomProperties()

    protected final def getEnvironmentVariable(name: String): String = {
      // Note: If the env variables are not there then it means that
      // setenv.sh script was not loaded properly. So, we can assume
      // that the value will always be there
      sys.env.get(name).get
    }

    final def start() {
      launcher.startApplication()
    }
}