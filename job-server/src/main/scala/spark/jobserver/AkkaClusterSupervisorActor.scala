package spark.jobserver

import java.nio.file.{Files, Paths}
import java.nio.charset.Charset
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.pattern.ask
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberEvent, MemberUp}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import spark.jobserver.util.{SparkJobUtils, ManagerLauncher}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import spark.jobserver.common.akka.InstrumentedActor

import scala.concurrent.Await
import akka.pattern.gracefulStop
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spark.jobserver.io.JobDAOActor.CleanContextJobInfos
import spark.jobserver.JobManagerActor.{GetContextData, ContextData, SparkContextDead}

import spark.jobserver.io.{JobDAOActor, ContextInfo, ContextStatus}

/**
 * The AkkaClusterSupervisorActor launches Spark Contexts as external processes
 * that connect back with the master node via Akka Cluster.
 *
 * Currently, when the Supervisor gets a MemberUp message from another actor,
 * it is assumed to be one starting up, and it will be asked to identify itself,
 * and then the Supervisor will try to initialize it.
 *
 * See the [[LocalContextSupervisorActor]] for normal config options.
 */
class AkkaClusterSupervisorActor(daoActor: ActorRef, dataManagerActor: ActorRef)
    extends InstrumentedActor {

  import ContextSupervisor._
  import scala.collection.JavaConverters._
  import scala.concurrent.duration._

  val config = context.system.settings.config
  val defaultContextConfig = config.getConfig("spark.context-settings")
  val contextInitTimeout = config.getDuration("spark.context-settings.context-init-timeout",
                                                TimeUnit.SECONDS)
  val contextDeletionTimeout = SparkJobUtils.getContextDeletionTimeout(config)
  import context.dispatcher
  var tempActorRef: ActorRef = _

  //actor name -> (context isadhoc, success callback, failure callback)
  //TODO: try to pass this state to the jobManager at start instead of having to track
  //extra state.  What happens if the WebApi process dies before the forked process
  //starts up?  Then it never gets initialized, and this state disappears.
//  private val contextInitInfos = mutable.HashMap.empty[String,
//                                                    (Config, Boolean, ActorRef => Unit, Throwable => Unit)]

  // actor name -> (JobManagerActor ref, ResultActor ref)
  private val contexts = mutable.HashMap.empty[String, (ActorRef, ActorRef)]

  private val cluster = Cluster(context.system)
  private val selfAddress = cluster.selfAddress
  val daoAskTimeout = Timeout(10 seconds)

  // This is for capturing results for ad-hoc jobs. Otherwise when ad-hoc job dies, resultActor also dies,
  // and there is no way to retrieve results.
  val globalResultActor = context.actorOf(Props[JobResultActor], "global-result-actor")

  logger.info("AkkaClusterSupervisor initialized on {}", selfAddress)

  override def preStart(): Unit = {
    cluster.join(selfAddress)
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
    cluster.leave(selfAddress)
  }

  def wrappedReceive: Receive = {
    case MemberUp(member) =>
      if (member.hasRole("manager")) {
        val memberActors = RootActorPath(member.address) / "user" / "*"
        context.actorSelection(memberActors) ! Identify(memberActors)
      }

    case ActorIdentity(memberActors, actorRefOpt) =>
      actorRefOpt.foreach{ actorRef =>
        val actorName = actorRef.path.name
        if (actorName.startsWith("jobManager")) {
          logger.info("Received identify response, attempting to initialize context at {}", memberActors)

          val contextId = actorName.replace("jobManager-", "")
          logger.info("[BEHROZ] Going to DB to get the contextInfo")
          val getContextResp = Await.result(
              (daoActor ? JobDAOActor.GetContextInfo(contextId))(daoAskTimeout).
                mapTo[JobDAOActor.ContextResponse], daoAskTimeout.duration)
          logger.info("[BEHROZ] Got response back")

          getContextResp.contextInfo match {
            case Some(contextInfo) =>
              logger.info("[BEHROZ] Reading config returned by DB")
              val contextConfig = ConfigFactory.parseString(contextInfo.config)

              val isAdhoc = contextConfig.getBoolean("is-adhoc")
              logger.info("[BEHROZ] Creating success and failure functions")
              val functions = getSuccessAndFailureFunctions(
                                actorRef,
                                contextConfig.getString("context.name"),
                                contextConfig.getString("originator") match {
                                  case "" => None
                                  case address => Some(address)
                                },
                                isAdhoc)

              initContext(contextConfig, actorName, actorRef,
                  contextInitTimeout, isAdhoc)(functions._1, functions._2)
              logger.info("[BEHROZ] Doing Init")
            case None =>
              logger.warn("No initialization or callback found for jobManager actor {}", actorRef.path)
              actorRef ! PoisonPill
          }
        } else {
          logger.info("[BBBBBBBBBBBBB====] " + actorRef.path.address.toString + "-" + actorRef.path.name)
        }
      }

    case AddContextsFromConfig =>
      addContextsFromConfig(config)

    case ListContexts =>
      sender ! contexts.keys.toSeq

    case GetSparkContextData(name) =>
      contexts.get(name) match {
        case Some((actor, _)) =>
          val future = (actor ? GetContextData)(30.seconds)
          val originator = sender
          future.collect {
            case ContextData(appId, Some(webUi)) =>
              originator ! SparkContextData(name, appId, Some(webUi))
            case ContextData(appId, None) => originator ! SparkContextData(name, appId, None)
            case SparkContextDead =>
              logger.info("SparkContext {} is dead", name)
              originator ! NoSuchContext
          }
        case _ => sender ! NoSuchContext
      }

    case AddContext(name, contextConfig) =>
      val originator = sender()
      val mergedConfig = contextConfig.withFallback(defaultContextConfig)
      // TODO(velvia): This check is not atomic because contexts is only populated
      // after SparkContext successfully created!  See
      // https://github.com/spark-jobserver/spark-jobserver/issues/349
      if (contexts contains name) {
        originator ! ContextAlreadyExists
      } else {
        tempActorRef = originator
        context.watch(originator)
        startContext(name, mergedConfig, false, Some(originator))
      }
//      logger.info(s"<<<<<BEHROZ>>>> ${originator.path.address} ${originator.path.name}")
//          logger.info(s"${originator.path.toStringWithoutAddress}")
//      val tempActorAddress =
//        RootActorPath(originator.path.address) / "temp" / originator.path.name
//      context.actorSelection(tempActorAddress) ! ContextInitialized
//      logger.info("After =>" + tempActorAddress.toStringWithAddress(originator.path.address))

    case StartAdHocContext(classPath, contextConfig) =>
      val originator = sender
      val mergedConfig = contextConfig.withFallback(defaultContextConfig)
      val userNamePrefix = Try(mergedConfig.getString(SparkJobUtils.SPARK_PROXY_USER_PARAM))
        .map(SparkJobUtils.userNamePrefix(_)).getOrElse("")
      var contextName = ""
      do {
        contextName = userNamePrefix + java.util.UUID.randomUUID().toString().take(8) + "-" + classPath
      } while (contexts contains contextName)
      // TODO(velvia): Make the check above atomic.  See
      // https://github.com/spark-jobserver/spark-jobserver/issues/349

      startContext(contextName, mergedConfig, true, Some(originator))

    case GetResultActor(name) =>
      sender ! contexts.get(name).map(_._2).getOrElse(globalResultActor)

    case GetContext(name) =>
      if (contexts contains name) {
        sender ! contexts(name)
      } else {
        sender ! NoSuchContext
      }

    case StopContext(name) =>
      if (contexts contains name) {
        logger.info("Shutting down context {}", name)
        val contextActorRef = contexts(name)._1
        cluster.down(contextActorRef.path.address)
        try {
          val stoppedCtx = gracefulStop(contexts(name)._1, contextDeletionTimeout seconds)
          Await.result(stoppedCtx, contextDeletionTimeout + 1 seconds)
          sender ! ContextStopped
        }
        catch {
          case err: Exception => sender ! ContextStopError(err)
        }
      } else {
        sender ! NoSuchContext
      }

    case Terminated(actorRef) =>
      val name: String = actorRef.path.name
      logger.info("Actor terminated: {}", name)
      logger.info("[BEHROZ] " + actorRef.path.address.toString + "-" + actorRef.path.toStringWithoutAddress)
      for ((name, _) <- contexts.find(_._2._1 == actorRef)) {
        contexts.remove(name)
        daoActor ! CleanContextJobInfos(name, DateTime.now())
      }
      cluster.down(actorRef.path.address)
  }

  private def initContext(contextConfig: Config,
                          actorName: String,
                          ref: ActorRef,
                          timeoutSecs: Long = 1,
                          isAdHoc: Boolean)
                          (successFunc: ActorRef => Unit, failureFunc: Throwable => Unit): Unit = {
    import akka.pattern.ask

    val resultActor = if (isAdHoc) globalResultActor else context.actorOf(Props(classOf[JobResultActor]))
    (ref ? JobManagerActor.Initialize(
      contextConfig, Some(resultActor), dataManagerActor))(Timeout(timeoutSecs.second)).onComplete {
      case Failure(e: Exception) =>
        logger.info("Failed to send initialize message to context " + ref, e)
        cluster.down(ref.path.address)
        ref ! PoisonPill
        failureFunc(e)
        updateContextStatus(actorName, Some(ref.path.address.toString), ContextStatus.Error, Some(e))
      case Success(JobManagerActor.InitError(t)) =>
        logger.info("Failed to initialize context " + ref, t)
        cluster.down(ref.path.address)
        ref ! PoisonPill
        failureFunc(t)
        updateContextStatus(actorName, Some(ref.path.address.toString), ContextStatus.Error, Some(t))
      case Success(JobManagerActor.Initialized(ctxName, resActor)) =>
        logger.info("SparkContext {} joined", ctxName)
        contexts(ctxName) = (ref, resActor)
        context.watch(ref)
        successFunc(ref)
        updateContextStatus(actorName, Some(ref.path.address.toString), ContextStatus.Running, None)
      case _ => logger.info("Failed for unknown reason.")
        cluster.down(ref.path.address)
        ref ! PoisonPill
        val e = new RuntimeException("Failed for unknown reason.")
        failureFunc(e)
        updateContextStatus(actorName, Some(ref.path.address.toString), ContextStatus.Error, Some(e))
    }
  }

    private def updateContextStatus(actorName: String, clusterAddress: Option[String],
      state: String, error: Option[Throwable]) = {
    import akka.pattern.ask
    val resp = Await.result(
      (daoActor ? JobDAOActor.GetContextInfo(actorName.replace("jobManager-", "")))(daoAskTimeout).
        mapTo[JobDAOActor.ContextResponse], daoAskTimeout.duration)

    val context = resp.contextInfo.get
    val endTime = error match {
      case None => context.endTime
      case _ => Some(DateTime.now())
    }
    daoActor ! JobDAOActor.SaveContextInfo(ContextInfo(context.id,
                                                       context.name,
                                                       context.config,
                                                       clusterAddress,
                                                       context.startTime,
                                                       endTime,
                                                       state,
                                                       error))
  }

  private def startContext(name: String, contextConfig: Config, isAdHoc: Boolean,
      originator: Option[ActorRef]): Unit = {
    require(!(contexts contains name), "There is already a context named " + name)

    val contextId = java.util.UUID.randomUUID().toString.substring(16)
    val contextActorName = "jobManager-" + contextId

    logger.info("Starting context with actor name {}", contextActorName)

    // Create a temporary dir, preferably in the LOG_DIR
    val encodedContextName = java.net.URLEncoder.encode(name, "UTF-8")
    val contextDir = Option(System.getProperty("LOG_DIR")).map { logDir =>
      Files.createTempDirectory(Paths.get(logDir), s"jobserver-$encodedContextName")
    }.getOrElse(Files.createTempDirectory("jobserver"))
    logger.info("Created working directory {} for context {}", contextDir: Any, name)
    import akka.serialization._
    val originatorAddress = originator match {
            case Some(ref) =>
              logger.info("[Behroz] => " + selfAddress.toString)
              logger.info("[Behroz] => " + ref.path.address.toString)
              logger.info("[Behroz] Host => " + ref.path.address.host)
              logger.info("[Behroz] Port => " + ref.path.address.port)
              logger.info("[Behroz] Protocol => " + ref.path.address.protocol)
              logger.info("[Behroz] System => " + ref.path.address.system)

              val identifier: String = Serialization.serializedActorPath(ref)
              identifier
            case None => ""
          }
    // Now create the contextConfig merged with the values we need
    val mergedContextConfig = ConfigFactory.parseMap(
      Map("is-adhoc" -> isAdHoc.toString,
          "context.name" -> name,
          "originator" -> originatorAddress).asJava
    ).withFallback(contextConfig)

    val contextInfo = ContextInfo(contextId, name,
        mergedContextConfig.root().render(ConfigRenderOptions.concise()), None,
        DateTime.now(), None, _: String, _: Option[Throwable])

    val launcher = new ManagerLauncher(config, contextConfig,
        selfAddress.toString, contextActorName, contextDir.toString)
    if (!launcher.start()) {
      val e = new Exception("Failed to launch context JVM");
      originator match {
        case Some(ref) => ref ! ContextInitError(e)
        case None => logger.error("Unable to start context" + name, e) // For adding contexts from config
      }

      daoActor ! JobDAOActor.SaveContextInfo(contextInfo(ContextStatus.Error, Some(e)))
    } else {
      daoActor ! JobDAOActor.SaveContextInfo(contextInfo(ContextStatus.Started, None))
    }
  }

  private def addContextsFromConfig(config: Config) {
    for (contexts <- Try(config.getObject("spark.contexts"))) {
      contexts.keySet().asScala.foreach { contextName =>
        val contextConfig = config.getConfig("spark.contexts." + contextName)
          .withFallback(defaultContextConfig)
        startContext(contextName, contextConfig, false, None)
      }
    }
  }

  private def getSuccessAndFailureFunctions(jobManagerActorRef: ActorRef, contextName: String,
      originatorAddress: Option[String], isAdhoc: Boolean): (ActorRef => Unit, Throwable => Unit) = {
     import scala.concurrent.duration.FiniteDuration

     val refInitTimeout = Duration(20, TimeUnit.SECONDS)

     val originatorRef: Option[ActorRef] = originatorAddress match {
       case Some(address) =>
         //val a = RootActorPath(selfAddress) / "temp" / address.replace("akka://JobServer/temp/", "")
         //logger.info("[Behroz] => Originator: " + a.toStringWithAddress(selfAddress))
         try {
//           val tempActorAddress =
//            RootActorPath(tempActorRef.path.address) / "temp" / tempActorRef.path.name
//           import akka.serialization._

//           val deserializedActorRef = extendedSystem.provider.resolveActorRef(address)
           // context.system.actorSelection("/temp/*") ! Identify(1)
            logger.info("[BEHROZ] test " + tempActorRef.path.toStringWithoutAddress + " -" +
               tempActorRef.path.address.toString)
           logger.info("[BEHROZ] Resolving temp actor " + address)
           val tt = context.system.actorSelection(address).resolveOne(refInitTimeout)
           val r = Some(
               Await.result(tt,
                   daoAskTimeout.duration))
           r
         } catch {
           case err: Exception => logger.error("[BEHROZ] EXCEPTION Timeout", err.getMessage)
           None
         }
       case None => None
     }

      var successFunc: (ActorRef => Unit) = { ref => }
      var failureFunc: (Throwable => Unit) = { e => }

      (originatorRef, isAdhoc) match {
        case (None, false) =>
          // Context creation from config
          failureFunc = { e => logger.error("Unable to start context" + contextName, e)}
        case (Some(origRef), true) =>
          // Adhoc Context
          successFunc = {ref => origRef ! jobManagerActorRef }
          failureFunc = {err => origRef ! ContextInitError(err)}
        case (Some(origRef), false) =>
          // Post /contexts
          successFunc = {ref => origRef ! ContextInitialized }
          failureFunc = {err => origRef ! ContextInitError(err)}
        case (None, true) =>
          // Unknown case
          logger.error("Unknown state. Originator reference is None and isAdhoc is set")
      }
      (successFunc, failureFunc)
    }
}
