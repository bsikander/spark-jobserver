package spark.jobserver.util

import java.io.File
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import org.apache.spark.launcher.SparkLauncher

class ContextLauncher(systemConfig: Config, contextConfig: Config,
    masterAddress: String, contextActorName: String, contextDir: String)
      extends Launcher(systemConfig) {
  val logger = LoggerFactory.getLogger("spark-context-launcher")

  override def addCustomArguments() {
      var loggingOpts = getEnvironmentVariable("MANAGER_LOGGING_OPTS")
      val configOverloads = getEnvironmentVariable("CONFIG_OVERRIDES")
      val sparkSubmitJavaOptions = getEnvironmentVariable("MANAGER_SPARK_SUBMIT_OPTIONS")
      var gcOPTS = baseGCOPTS

      if (deployMode == "client") {
        val gcFilePath = new File(contextDir, getEnvironmentVariable("GC_OUT_FILE_NAME")).toString()
        loggingOpts += s" -DLOG_DIR=$contextDir"
        gcOPTS += s" -Xloggc:$gcFilePath"
      }
      launcher.setVerbose(true)
      launcher.setMainClass("spark.jobserver.JobManager")
      launcher.addAppArgs(masterAddress, contextActorName, getEnvironmentVariable("MANAGER_CONF_FILE"))
      launcher.addSparkArg("--driver-memory", getEnvironmentVariable("JOBSERVER_MEMORY"))
      launcher.addSparkArg("--conf", s"spark.executor.extraJavaOptions=$loggingOpts")
      launcher.addSparkArg("--driver-java-options",
          s"$gcOPTS $baseJavaOPTS $loggingOpts $configOverloads $sparkSubmitJavaOptions")

      if (contextConfig.hasPath(SparkJobUtils.SPARK_PROXY_USER_PARAM)) {
         launcher.addSparkArg("--proxy-user", contextConfig.getString(SparkJobUtils.SPARK_PROXY_USER_PARAM))
      }

      val submitOutputFile = new File(contextDir, "spark-job-server.out")
      // This function was introduced in 2.1.0 Spark version, for older versions, it will
      // break with MethodNotFound exception.
      launcher.redirectOutput(submitOutputFile)
      launcher.redirectError(submitOutputFile)
    }
}
