package sample.cluster.AllReduce

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.collection.mutable.ArrayBuffer

class DataBuffer {
	var data_buff = new ArrayBuffer[Array[Double]]();
	var ids = new ArrayBuffer[Int]();

	var gap = -1;

	def init(thisGap:Int) = {
		gap = thisGap;
		data_buff += Array.empty;
		ids += 0;
	}

	//return true if we insert, else return false
	def insert_data(newData:Array[Double], newID: Int) = {
		var latest_id = ids.last;
		for (i:Int <- latest_id+1 until newID+1){
			data_buff += Array.empty;
			ids += i;
		}
		while (ids.length > gap){
			ids.remove(0);
			data_buff.remove(0);
		}
		var index = newID - ids.head;
		var isSuccessful = false;
		if (index >= 0){
			data_buff(index) = newData;
			isSuccessful = true;
		}
		(isSuccessful);
	}


	//---------helper functions ----------//

	def get_index(id:Int) = {
		(id - ids.head);
	}

	//check if it is a valid index, within the buffer range
	def is_valid_index(id:Int) = {
		if (ids.isEmpty)
			(false)
		else
			(id >= 0 && id < gap)
	}

	def get_data(id: Int) = {
		var index = get_index(scala.math.max(0, id-1));
		var result:Array[Double] = Array.empty;
		if (is_valid_index(index)){
			result = data_buff(index);
		}
		(result);
	}

	def printInfo(): Unit = {
		println(s"Data Buffer Size ${data_buff.length};  ID Buffer Size ${ids.length}");
		for (i:Int <- 0 until ids.length){
			println(s"id: ${ids(i)}, data: ${data_buff(i).mkString(",")}");
		}
		println(s"-----------");

	}
};