package sample.cluster.AllReduce
import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps


object Test {
  def main(args: Array[String]): Unit = {
  	var multiBuffers = new DataBuffer();
	multiBuffers.init(5);

	multiBuffers.insert_data(Array(1,1,1,1),1);
	multiBuffers.printInfo();

	multiBuffers.insert_data(Array(0,0,0,0),0);
	multiBuffers.printInfo();

	multiBuffers.insert_data(Array(4,4,4,4),4);
	multiBuffers.printInfo();


	multiBuffers.insert_data(Array(3,3,3,3),3);
	multiBuffers.printInfo();

	multiBuffers.insert_data(Array(8,8,8,8),8);
	multiBuffers.printInfo();

	multiBuffers.insert_data(Array(2,2,2,2),2);
	multiBuffers.printInfo();


  }
}