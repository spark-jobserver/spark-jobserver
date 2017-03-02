package spark.jobserver

import akka.actor.ActorRef
import akka.cluster.Member
import com.typesafe.config.Config

import spark.jobserver.util.SparkJobUtils

/**
 * This class starts and stops JobManagers / Contexts in-process.
 * It is responsible for watching out for the death of contexts/JobManagers.
 *
 * == Auto context start configuration ==
 * Contexts can be configured to be created automatically at job server initialization.
 * Configuration example:
 * {{{
 *   spark {
 *     contexts {
 *       olap-demo {
 *         num-cpu-cores = 4            # Number of cores to allocate.  Required.
 *         memory-per-node = 1024m      # Executor memory per node, -Xmx style eg 512m, 1G, etc.
 *       }
 *     }
 *   }
 * }}}
 *
 * == Other configuration ==
 * {{{
 *   spark {
 *     jobserver {
 *       context-creation-timeout = 15 s
 *       yarn-context-creation-timeout = 40 s
 *     }
 *
 *     # Default settings for all context creation
 *     context-settings {
 *       spark.mesos.coarse = true
 *     }
 *   }
 * }}}
 */
class LocalContextSupervisorActor(dao: ActorRef) extends BaseSupervisorActor {

  val localContextTimeout = SparkJobUtils.getContextTimeout(config)

  protected def startContext(
    name: String, actorName: String, contextConfig: Config, isAdHoc: Boolean
  )(successFunc: ActorRef => Unit, failureFunc: Throwable => Unit) = {

    val mergedConfig = createMergedActorConfig(name, actorName, contextConfig, isAdHoc)
    val ref = context.actorOf(JobManagerActor.props(mergedConfig, dao), actorName)
    initContext(actorName, ref, getTimeout())(isAdHoc, successFunc, failureFunc)
  }

  protected def getTimeout(): Long = localContextTimeout.toLong
  // both no-ops here
  protected def onMemberUp(member: Member): Unit = {}
  protected def onActorIdentity(memberActors: Any, actorRefOpt: Option[ActorRef]): Unit = {}
  protected def onStopContext(actor: ActorRef): Unit = {}

}
