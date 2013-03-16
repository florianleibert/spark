package spark.executor

import java.nio.ByteBuffer
import spark.{DriverChangeListener, DriverFinder, Logging}
import spark.TaskState.TaskState
import spark.util.AkkaUtils
import akka.actor.{ActorRef, Actor, Props, Terminated}
import akka.remote.{RemoteClientLifeCycleEvent, RemoteClientShutdown, RemoteClientDisconnected}
import java.util.concurrent.{TimeUnit, ThreadPoolExecutor, SynchronousQueue}
import spark.scheduler.cluster._
import spark.scheduler.cluster.RegisteredExecutor
import spark.scheduler.cluster.LaunchTask
import spark.scheduler.cluster.RegisterExecutorFailed
import spark.scheduler.cluster.RegisterExecutor

private[spark] class StandaloneExecutorBackend(
    executor: Executor,
    driverUrl: String,
    executorId: String,
    hostname: String,
    cores: Int)
  extends Actor
  with ExecutorBackend
  with Logging {

  var driver: ActorRef = null
  var registered = false
  var currentDriverUrl: String = _

  def register(driverUrl: String) {
    logInfo("Connecting to driver: " + driverUrl)
    currentDriverUrl = driverUrl
    driver = context.actorFor(driverUrl)
    driver ! RegisterExecutor(executorId, hostname, cores)
    context.system.eventStream.subscribe(self, classOf[RemoteClientLifeCycleEvent])
    context.watch(driver) // Doesn't work with remote actors, but useful for testing
  }

  override def preStart() {
    register(driverUrl)
    val masterFinder = new DriverFinder(driverUrl)
    masterFinder.addListener(new DriverChangeListener {
      def onMasterChanged(newDriverUrl: String) {
        register(newDriverUrl)
      }
    })
  }

  override def receive = {
    case RegisteredExecutor(sparkProperties) =>
      logInfo("Successfully registered with driver")
      if (!registered) {
        executor.initialize(executorId, hostname, sparkProperties)
        registered = true
      } else {
        executor.driverChanged(currentDriverUrl)
      }

    case RegisterExecutorFailed(message) =>
      logError("Slave registration failed: " + message)
      System.exit(1)

    case LaunchTask(taskDesc) =>
      logInfo("Got assigned task " + taskDesc.taskId)
      executor.launchTask(this, taskDesc.taskId, taskDesc.serializedTask)

    case Terminated(x) =>
      logError("Driver terminated: " + x)

    case RemoteClientDisconnected(x, y) =>
      logError("Driver disconnected: " + x + " " + y)

    case RemoteClientShutdown(x, y) =>
      logError("Driver shutdown: " + x + " " + y)
  }

  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {
    driver ! StatusUpdate(executorId, taskId, state, data)
  }
}

private[spark] object StandaloneExecutorBackend {
  def run(driverUrl: String, executorId: String, hostname: String, cores: Int) {
    // Create a new ActorSystem to run the backend, because we can't create a SparkEnv / Executor
    // before getting started with all our system properties, etc
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("sparkExecutor", hostname, 0)
    val actor = actorSystem.actorOf(
      Props(new StandaloneExecutorBackend(new Executor, driverUrl, executorId, hostname, cores)),
      name = "Executor")
    actorSystem.awaitTermination()
  }

  def main(args: Array[String]) {
    if (args.length < 4) {
      //the reason we allow the last frameworkId argument is to make it easy to kill rogue executors
      System.err.println("Usage: StandaloneExecutorBackend <driverUrl> <executorId> <hostname> <cores> [<appid>]")
      System.exit(1)
    }
    run(args(0), args(1), args(2), args(3).toInt)
  }
}
