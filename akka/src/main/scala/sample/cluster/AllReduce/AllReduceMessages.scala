package sample.cluster.AllReduce
import akka.actor.ActorRef

final case class Scatter(data: Array[Double], data_id: Int, iteration_id: Int)

final case class Gather(data: Array[Double], data_id: Int, iteration_id: Int)

final case class StartAllReduce(iteration_id: Int)

final case class StartScatter(iteration_id: Int)

final case class AllReduceDone(iteration_id: Int)

final case class InitializeWorker(refs: collection.mutable.Map[Integer, ActorRef] , initialized_data: Array[Double])

final case class WorkerReady()
