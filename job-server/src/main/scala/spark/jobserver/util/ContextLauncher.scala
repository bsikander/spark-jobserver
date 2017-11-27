package spark.jobserver.util

import com.typesafe.config.Config
import org.slf4j.LoggerFactory

class ContextLauncher(systemConfig: Config, contextConfig: Config,
    masterAddress: String, contextActorName: String, contextDir: String)
      extends Launcher(systemConfig) {
  val logger = LoggerFactory.getLogger("spark-context-launcher")

  override def addCustomArguments() {
      val loggingOpts =
        s"-Dlog4j.configuration=file:$currentWorkingDirectory/log4j-server.properties -DLOG_DIR=$contextDir"
      val gcOPTSManager = s"$baseGCOPTS -Xloggc:$contextDir/" + getEnvironmentVariable("GC_OUT_FILE_NAME")
      val configOverloads = getEnvironmentVariable("CONFIG_OVERRIDES")
      val sparkSubmitJavaOptions = getEnvironmentVariable("SPARK_SUBMIT_JAVA_OPTIONS")

      launcher.setMainClass("spark.jobserver.JobManager")
      launcher.addAppArgs(masterAddress, contextActorName, getEnvironmentVariable("conffile"))
      launcher.addSparkArg("--driver-memory", getEnvironmentVariable("JOBSERVER_MEMORY"))
      launcher.addSparkArg("--conf", s"spark.executor.extraJavaOptions=$loggingOpts")
      launcher.addSparkArg("--driver-java-options",
          s"$gcOPTSManager $baseJavaOPTS $loggingOpts $configOverloads $sparkSubmitJavaOptions")

      if (contextConfig.hasPath(SparkJobUtils.SPARK_PROXY_USER_PARAM)) {
         launcher.addSparkArg("--proxy-user", contextConfig.getString(SparkJobUtils.SPARK_PROXY_USER_PARAM))
      }

      val keyTab = getEnvironmentVariable("JOBSERVER_KEYTAB")
      if (keyTab != "") launcher.addSparkArg("--keytab", keyTab)
    }
}