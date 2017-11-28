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
      val log4jPath = new File(currentWorkingDirectory, "log4j-server.properties").toString()
      val gcFilePath = new File(contextDir, getEnvironmentVariable("GC_OUT_FILE_NAME")).toString()
      val loggingOpts =
        s"-Dlog4j.configuration=file:$log4jPath -DLOG_DIR=$contextDir"
      val gcOPTSManager = s"$baseGCOPTS -Xloggc:$gcFilePath"
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
