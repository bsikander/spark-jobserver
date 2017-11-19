package spark.jobserver.util

import com.typesafe.config.Config

class ContextLauncher(systemConfig: Config, contextConfig: Config, masterAddress: String, contextActorName: String) extends Launcher(systemConfig) {

    def addCustomProperties() {
      launcher.setMainClass("spark.jobserver.JobManager")
      launcher.addAppArgs(masterAddress, contextActorName, "/Users/d068274/Documents/dev_sqldao.conf")
      //launcher.addSparkArg("--conf",
       // TODO: Fix this
      //"spark.executor.extraJavaOptions=-Dlog4j.configuration=file:$CONF_DIR/log4j-server.properties
      //-DLOG_DIR=")
      //val driverJavaOptions =
      // "-XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCTimeStamps -XX:MaxPermSize=512m
      //-XX:+CMSClassUnloadingEnabled "
      //launcher.addSparkArg("--driver-java-options", driverJavaOptions)
      //"/Users/d068274/Documents/Projects/git/spark-jobserver-opensource" +
      //    "/job-server-extras/target/scala-2.11/spark-job-server.jar"
      launcher.addSparkArg("--driver-memory", "1G")
    }
}