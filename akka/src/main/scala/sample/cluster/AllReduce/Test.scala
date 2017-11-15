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
  	//testScatterBuffer();
  	testDataBuffer();
  }

  def testDataBuffer(): Unit = {
  	var dataBuffer = new DataBuffer();
  	dataBuffer.init(2);
  	var arr = Array(0.0,2,3,4);
  	dataBuffer.insert_data(arr, 0);
  	dataBuffer.printInfo();
  	arr = Array(1.0,2,3,4)
  	dataBuffer.insert_data(arr, 1);
  	dataBuffer.printInfo();
  	arr = Array(2.0,3,4,5)
  	dataBuffer.insert_data(arr, 2);
  	dataBuffer.printInfo();
  	dataBuffer.insert_data(Array(3,4,5,6), 3);
  	dataBuffer.printInfo();
  	dataBuffer.insert_data(Array(4,5,6,7), 4);
  	dataBuffer.printInfo();
  	dataBuffer.insert_data(Array(2,3,4,5), 4);
  	dataBuffer.printInfo();

  }

  def testScatterBuffer(): Unit = {
  	var scatterBuffer = new ScatterBuffer();
	scatterBuffer.init(5, 0.5, 7);

	scatterBuffer.insert_data(Array(1,1,1,1),1);
	scatterBuffer.insert_data(Array(1,1,1,1),1);
	scatterBuffer.insert_data(Array(1,1,1,1),1);
	scatterBuffer.insert_data(Array(1,1,1,1),1);
	println(s"-----------");
	if (scatterBuffer.can_reduce_data(1)){
		println(s"${scatterBuffer.reduce_data(1).mkString(",")}")
	}else{
		println(s"Cannot Reduce Data 1");
	}
	println(s"-----------");
	

	scatterBuffer.insert_data(Array(0,0,0,0),0);
	scatterBuffer.insert_data(Array(0,0,0,0),0);
	scatterBuffer.insert_data(Array(0,0,0,0),0);
	println(s"-----------");
	if (scatterBuffer.can_reduce_data(0)){
		println(s"${scatterBuffer.reduce_data(0).mkString(",")}")
	}else{
		println(s"Cannot Reduce Data 0");
	}
	println(s"-----------");

	scatterBuffer.insert_data(Array(4,4,4,4),4);
	scatterBuffer.insert_data(Array(4,4,4,4),4);
	scatterBuffer.insert_data(Array(4,4,4,4),4);
	scatterBuffer.insert_data(Array(4,4,4,4),4);
	scatterBuffer.insert_data(Array(4,4,4,4),4);
	scatterBuffer.insert_data(Array(4,4,4,4),4);
	println(s"-----------");
	if (scatterBuffer.can_reduce_data(4)){
		println(s"${scatterBuffer.reduce_data(4).mkString(",")}")
	}else{
		println(s"Cannot Reduce Data 4");
	}
	println(s"-----------");
	
	scatterBuffer.insert_data(Array(3,3,3,3),3);
	println(s"-----------");
	if (scatterBuffer.can_reduce_data(3)){
		println(s"${scatterBuffer.reduce_data(3).mkString(",")}")
	}else{
		println(s"Cannot Reduce Data 3");
	}
	println(s"-----------");

	scatterBuffer.insert_data(Array(8,8,8,8),8);
	scatterBuffer.insert_data(Array(8,8,8,8),8);
	
	// println(s"-----------");
	// if (scatterBuffer.can_reduce_data(8)){
	// 	println(s"${scatterBuffer.reduce_data(8).mkString(",")}")
	// }else{
	// 	println(s"Cannot Reduce Data 8");
	// }
	// println(s"-----------");

	scatterBuffer.insert_data(Array(2,2,2,2),2);
	scatterBuffer.insert_data(Array(2,2,2,2),2);
	scatterBuffer.insert_data(Array(2,2,2,2),2);
	scatterBuffer.insert_data(Array(2,2,2,2),2);
	scatterBuffer.insert_data(Array(2,2,2,2),2);
	println(s"-----------");
	if (scatterBuffer.can_reduce_data(2)){
		println(s"${scatterBuffer.reduce_data(2).mkString(",")}")
	}else{
		println(s"Cannot Reduce Data 2");
	}
	println(s"-----------");

	scatterBuffer.insert_data(Array(9,9,9,9),9);
	scatterBuffer.insert_data(Array(9,9,9,9),9);
	scatterBuffer.insert_data(Array(9,9,9,9),9);
	scatterBuffer.insert_data(Array(9,9,9,9),9);
	scatterBuffer.insert_data(Array(9,9,9,9),9);
	scatterBuffer.insert_data(Array(9,9,9,9),9);
	println(s"-----------");
	if (scatterBuffer.can_reduce_data(9)){
		println(s"${scatterBuffer.reduce_data(9).mkString(",")}")
	}else{
		println(s"Cannot Reduce Data 9");
	}
	println(s"-----------");

	scatterBuffer.insert_data(Array(8,8,8,8),8);
	scatterBuffer.insert_data(Array(8,8,8,8),8);
	println(s"-----------");
	if (scatterBuffer.can_reduce_data(8)){
		println(s"${scatterBuffer.reduce_data(8).mkString(",")}")
	}else{
		println(s"Cannot Reduce Data 8");
	}
	println(s"-----------");
	scatterBuffer.insert_data(Array(8,8,8,8),8);
	if (scatterBuffer.can_reduce_data(8)){
		println(s"${scatterBuffer.reduce_data(8).mkString(",")}")
	}else{
		println(s"Cannot Reduce Data 8");
	}
	println(s"-----------");


  }
}