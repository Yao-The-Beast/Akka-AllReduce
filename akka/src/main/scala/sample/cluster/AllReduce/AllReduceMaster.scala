package sample.cluster.AllReduce

import akka.Done
import akka.actor.{Actor, ActorRef, ActorSystem, Props, RootActorPath, Terminated}
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberUp}
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import java.time._


class AllReduceMaster(num_workers:Int) extends Actor {

  var allReduceDone = 0;

  //current iteration id, starts from 0
  var iteration_id = 0;

  var workers: collection.mutable.Map[Integer, ActorRef] = collection.mutable.Map[Integer, ActorRef]()

  val cluster = Cluster(context.system)
  override def preStart(): Unit = cluster.subscribe(self, classOf[MemberUp])
  override def postStop(): Unit = cluster.unsubscribe(self)

  var num_ready_workers = 0


  def receive = {

    case MemberUp(m) =>
      println(s"detect member up, currently ${workers.size}")
      println(s"Member is Up: ${m.address}")
      register(m).onSuccess {
        case Done =>
          if (workers.size == num_workers) {
            println(s"expected members up, start broadcast initial value")
            //initialize all workers
            initialize_workers();
          }
      }

    case worker_ready: WorkerReady =>
      num_ready_workers += 1;
      if (num_ready_workers == num_workers){
        self ! StartAllReduce(iteration_id);
      }

    case start : StartAllReduce =>
      for ((idx, worker) <- workers){
        worker ! StartScatter(start.iteration_id)
      }

    case iterationFinish: AllReduceDone =>
      //if the iterationFinish msg sent from one actor has the same iteration id
      //we increase the counter
      if (iterationFinish.iteration_id == iteration_id){
        allReduceDone += 1;
        if (allReduceDone == num_workers){
          //debug use
          Thread.sleep(500);
          iteration_id += 1;
          println(s"Next Round ${iteration_id}");
          allReduceDone = 0;
          self ! StartAllReduce(iteration_id);
        }
      }else{
        println(s"Stale Iteration Finish from ${sender.toString()}");
      }
  }

//------------------Helper Functions------------------//

  def initialize_workers(): Unit = {

    var data:Array[Array[Double]] = Array.empty;
    for (i <- 0 until num_workers){
      data :+= Array(i.toDouble,i.toDouble,i.toDouble,i.toDouble, i.toDouble, i.toDouble);
    }
    for((idx, worker) <- workers){
      worker ! InitializeWorker(workers, data(idx));
    }

  }

  def register(member: Member): Future[Done] =
    if (member.hasRole("worker")) {
      implicit val timeout = Timeout(5.seconds)
      context.actorSelection(RootActorPath(member.address) / "user" / "worker").resolveOne().map { workerRef =>
          context watch workerRef
          val new_idx: Integer = workers.size
          workers.update(new_idx, workerRef)
          Done
      }
    } else {
      Future.successful(Done)
    }
}



object AllReduceMaster {
  def main(args: Array[String]= Array("2551", "3")): Unit = {
    
    val port = if (args.isEmpty) "2551" else args(0)
    val num_workers = if (args.length < 2) 3 else args(1).toInt

    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").
      withFallback(ConfigFactory.parseString("akka.cluster.roles = [master]")).
        withFallback(ConfigFactory.load())

    val system = ActorSystem("ClusterSystem", config)
    val master = system.actorOf(Props(classOf[AllReduceMaster], num_workers), name = "master")
  
  }
}

