### Note
Cluster mode works with Spark 2+. Older versions of Spark fail in cluster mode due to the following bug.
https://issues.apache.org/jira/browse/SPARK-14699

### Configuring Jobserver for standalone cluster mode of Spark
Spark jobserver can be configured to start the driver processes in Spark cluster. Configuring this option is straightforward and requires the following steps.

- Set `spark.master` config parameter with the address of your standalone Spark cluster. e.g. `spark://<master>:7077,<secondary_master>:7077`
- Set `spark.jobserver.driver-mode` config parameter to `standalone-cluster`
- Set `akka.remote.netty.tcp.hostname` to the current IP of host (sjs/slave). Do not use `127.0.0.1` or `localhost`. Since driver is launched in Spark cluster, local.conf on slave should contain the IP of the current slave. local.conf on spark jobserver VM should contain the IP of sjs.
- Set `akka.remote.netty.tcp.maximum-frame-size` to `104857600b` to support big jars fetching from remote.
- Set `spark.jobserver.context-per-jvm` to `true`.
- Set `JobDao` to SqlDAO with [proper](https://github.com/spark-jobserver/spark-jobserver#context-per-jvm) connection configurations.

Here is an example configuration
```
spark {
  # spark.master will be passed to each job's JobContext
  master = "spark://<master>:7077,<secondary_master>:7077"

  jobserver {
    ...
    context-per-jvm = true
    driver-mode = standalone-cluster
    jobdao = spark.jobserver.io.JobSqlDAO
    ...
    sqldao {
      # Slick database driver, full classpath
      slick-driver = slick.driver.H2Driver

      # JDBC driver, full classpath
      jdbc-driver = org.h2.Driver

      # Directory where default H2 driver stores its data. Only needed for H2.
      rootdir = <path>

      # Full JDBC URL / init string, along with username and password.  Sorry, needs to match above.
      # Substitutions may be used to launch job-server, but leave it out here in the default or tests won't pass
      jdbc {
        url = "jdbc:h2:tcp://<IP>//<db_path>"
        user = ""
        password = ""
      }

      # DB connection pool settings
      dbcp {
        enabled = false
        maxactive = 20
        maxidle = 10
        initialsize = 10
      }
    }
    ...
  }
  ...
}
...
akka {
  remote {
    netty.tcp {
       # Configure the maximum message size, including job results, that can be sent
       maximum-frame-size = 104857600b
       hostname = "<Host IP>" #This should be the IP of the current VM
    }
  }
}
...
```

### Helpful Links
- [Standalone cluster installation](https://spark.apache.org/docs/latest/spark-standalone.html)
- [Cluster mode overview](https://spark.apache.org/docs/latest/cluster-overview.html)

