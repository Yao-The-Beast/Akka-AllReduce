package sample.cluster.AllReduce

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps


class AllReduceWorker extends Actor {
 
  var scatter_buff: Array[Array[Double]] = Array.empty
  var output_buff : Array[Double] = Array.empty
  var segment_count = 0
  var data_size = 0;
  var group_size = 0;
  var segment_size = 0;
  var idx = -1;
  var master:Array[ActorRef] = Array.empty;

  val group: collection.mutable.Map[Integer, ActorRef] = collection.mutable.Map[Integer, ActorRef]()
  var data: Array[Double] = Array.empty

  var iteration_id = 0;


  def receive = {

    case initialize_package : InitializeWorker => 
      
      for ((index, thisWorker) <- initialize_package.refs){
        group += (index -> thisWorker)
        if (self == thisWorker){
          idx = index
        }
      }

      data = initialize_package.initialized_data;
      data_size = data.length;
      group_size = group.size;
      segment_size = data_size / group_size;
      output_buff = Array.fill[Double](data_size)(0.0);

      println(s"Peers and Data initialized");
      
      master :+= sender;
      //let master know I am ready
      sender ! WorkerReady();



    case start: StartScatter =>
      iteration_id = start.iteration_id;
      segment_count = 0;
      for ((index, worker) <- group) {
        val (data_begin, data_end) = getRange(index)
        val sliced: Array[Double] = data.slice(data_begin, data_end)
        worker ! Scatter(sliced, index, start.iteration_id)
      }

    case scatter: Scatter =>
      assert(scatter.data_id == idx)
      scatter_buff :+= scatter.data
      if(scatter_buff.length == group_size){
        //reduce the scatter_buff
        val reduced_result = reduceData();
        scatter_buff = Array.empty;
        //broadcast
        for (index:Int <- 0 until group_size){
          group(index) ! Gather(reduced_result, idx, scatter.iteration_id)
        }
      }

    case gather: Gather =>
      val (data_begin, data_end) = getRange(gather.data_id)
      for (i:Int <- data_begin until data_end) {
        output_buff(i) = gather.data(i-data_begin)
      }
      segment_count += 1
      if(segment_count == group_size){
        data = output_buff.clone()
        println(s"------------ Reduce Done--------------");
        printData();
        master(0) ! AllReduceDone(iteration_id)
      }

    case _ => // ignore
  }


//--------------HELPER FUNCTION----------------//

  //return [start index , end index]
  def getRange(data_id: Int) : (Int, Int) = {
    assert(data_id < data_size)
    (segment_size * data_id, scala.math.min(segment_size * (data_id + 1), data_size))
  }

  //reduce data
  def reduceData() = {
    assert (scatter_buff.length == group_size);
    //right now we just add them up
    var reduced_buff:Array[Double] = Array.empty;
    for (i <- 0 until scatter_buff.length){
      if (reduced_buff.isEmpty){
        reduced_buff = scatter_buff(i);
      }else{
        for (j <- 0 until scala.math.min(reduced_buff.length, scatter_buff(i).length)){
          reduced_buff(j) += scatter_buff(i)(j);
          reduced_buff(j) = scala.math.min(reduced_buff(j),1000);
        }
      }
    }
    (reduced_buff)

  }

  def printInfo() = {
    println(s"Worker Index: ${idx}");
    println(s"Data: ${data}");
  }

  def printData() = {
    for (i:Int <- 0 until data.length){
      println(s"${data(i)}");
    }
    println(s"Iteration id: ${iteration_id}");
  }
}


object AllReduceWorker {
  def main(args: Array[String]): Unit = {
    // Override the configuration of the port when specified as program argument
    val port = if (args.isEmpty) "0" else args(0)
    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").
      withFallback(ConfigFactory.parseString("akka.cluster.roles = [worker]")).
      withFallback(ConfigFactory.load())
    //src/resources/conf/application.conf

    val system = ActorSystem("ClusterSystem", config)
    val worker = system.actorOf(Props[AllReduceWorker], name = "worker")
  }
}
