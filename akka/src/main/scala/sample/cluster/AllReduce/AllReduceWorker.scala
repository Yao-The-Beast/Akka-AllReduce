package sample.cluster.AllReduce

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps


class AllReduceWorker extends Actor {
 
  
  var output_buff : Array[Double] = Array.empty
  var segment_count = 0
  var data_size = 0;
  var group_size = 0;
  var segment_size = 0;
  var idx = -1;
  var master:Array[ActorRef] = Array.empty;

  val group: collection.mutable.Map[Integer, ActorRef] = collection.mutable.Map[Integer, ActorRef]()
  var data_buff = new DataBuffer();
  var scatter_buff = new ScatterBuffer();


  var iteration_id = 0;

  //about multibuffer stuff
  var scatter_buff_threshold = 0.5;
  var gather_buff_threshold = 1;
  var maximum_gap = 2;


  def receive = {

    case initialize_package : InitializeWorker => 
      
      for ((index, thisWorker) <- initialize_package.refs){
        group += (index -> thisWorker)
        if (self == thisWorker){
          idx = index
        }
      }

      //initialize data_buff, scatter_buff
      data_buff.init(maximum_gap);
      scatter_buff.init(maximum_gap, scatter_buff_threshold, group.size);

      data_buff.insert_data(initialize_package.initialized_data, iteration_id);
      data_size = initialize_package.initialized_data.length;
      group_size = group.size;
      segment_size = data_size / group_size;
      output_buff = Array.fill[Double](data_size)(0.0);

      println(s"Peers and data_buff initialized");

      master :+= sender;
      //let master know I am ready
      sender ! WorkerReady();



    case start: StartScatter =>
      iteration_id = start.iteration_id;
      for ((index, worker) <- group) {
        val (data_begin, data_end) = getRange(index)
        val sliced: Array[Double] = data_buff.get_data(start.iteration_id).slice(data_begin, data_end)
        worker ! Scatter(sliced, index, start.iteration_id)
      }

    case scatter: Scatter =>
      assert(scatter.data_id == idx)
      scatter_buff.insert_data(scatter.data, scatter.iteration_id);
      //debug
      //println(s"From ${sender.toString()}     Value ${scatter.data.mkString(",")}")

      //if we can reduce data, enough messages have been gathered
      if(scatter_buff.can_reduce_data(scatter.iteration_id)){
        //reduce the scatter_buff
        val reduced_result = scatter_buff.reduce_data(scatter.iteration_id);
        //broadcast
        for (index:Int <- 0 until group_size){
          group(index) ! Gather(reduced_result, idx, scatter.iteration_id)
        }
      }

    case gather: Gather =>
      val (data_begin, data_end) = getRange(gather.data_id)
      //println(s"Gather From ${sender.toString()}")
      for (i:Int <- data_begin until data_end) {
        output_buff(i) = gather.data(i-data_begin)
      }
      segment_count += 1
      if(segment_count == group_size){
        segment_count = 0;
        data_buff.insert_data(output_buff, gather.iteration_id)
        println(s"------------ Reduce Done--------------");
        data_buff.printInfo();
        output_buff = Array.fill[Double](data_size)(0.0);
        master(0) ! AllReduceDone(gather.iteration_id)
      }

    case _ => // ignore
  }


//--------------HELPER FUNCTION----------------//

  //return [start index , end index]
  def getRange(data_id: Int) : (Int, Int) = {
    assert(data_id < data_size)
    (segment_size * data_id, scala.math.min(segment_size * (data_id + 1), data_size))
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
