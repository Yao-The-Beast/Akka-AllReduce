package sample.cluster.AllReduce

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.collection.mutable.ArrayBuffer

class ScatterBuffer {

	var scatter_buff = new ArrayBuffer[Array[Array[Double]]]();
	var ids = new ArrayBuffer[Int]();
	var has_reduced = new ArrayBuffer[Int]();	//-1 not yet, 1 has
	var threshold = 1.0;
	var gap = -1;
	var group_size = -1;

	def init(thisGap:Int, thresHold: Double, groupSize: Int) = {
		gap = thisGap;
		scatter_buff += Array.empty;
		ids += 0;
		has_reduced += -1
		threshold = thresHold;
		group_size = groupSize;
	}

	//return true if we insert, else return false
	def insert_data(newData:Array[Double], newID: Int) = {
		var latest_id = ids.last;
		for (i:Int <- latest_id+1 until newID+1){
			scatter_buff += Array.empty;
			ids += i;
			has_reduced += -1;
		}
		while (ids.length > gap){
			ids.remove(0);
			has_reduced.remove(0);
			scatter_buff.remove(0);
		}
		var index = newID - ids.head;
		var isSuccessful = false;
		//we can insert data
		if (can_insert(index)){
			scatter_buff(index) :+= newData;
			isSuccessful = true;
		}
		(isSuccessful);
	}

	//safe guard function to see if we can reduce the scatter_buff
	def can_reduce_data(iteration_id:Int) = {
		var index = get_index(iteration_id);
		if (!is_valid_index(index) || has_been_reduced(index)){
			(false)
		}else{
			if (threshold * group_size <= scatter_buff(index).length){
				has_reduced(index) = 1;
				(true)
			}else
				(false)
		}
	}

	//reduce data if scatter_buff is eligible to reduce
	def reduce_data(iteration_id:Int) = {
	    //right now we just add them up
	    var index = get_index(iteration_id);
	    var reduced_buff:Array[Double] = Array.empty;
	    for (i <- 0 until scatter_buff(index).length){
	      if (reduced_buff.isEmpty){
	        reduced_buff = scatter_buff(index)(i);
	      }else{
	        for (j <- 0 until scala.math.min(reduced_buff.length, scatter_buff(index)(i).length)){
	          reduced_buff(j) += scatter_buff(index)(i)(j);
	        }
	      }
	    }
	    (reduced_buff)
	}


	//---------helper functions ----------//

	//convert iteration_id to the index within the scatter_buff
	def get_index(id:Int) = {
		(id - ids.head);
	}

	//check if it is a valid index 
	def is_valid_index(id:Int) = {
		if (ids.isEmpty)
			(false)
		else
			(id >= 0 && id < gap)
	}

	//check if has been reduce at this index
	def has_been_reduced(id:Int) = {
		(has_reduced(id) == 1);
	}

	//check if we can insert more data into the scatter_buff
	def can_insert(id:Int) = {
		if (is_valid_index(id) && scatter_buff(id).length < group_size)
			(true)
		else
			(false)
	}

	def printInfo(): Unit = {
		println(s"Scatter Buffer Size ${scatter_buff.length};  ID Buffer Size ${ids.length}");
		for (i:Int <- 0 until ids.length){
			println(s"id: ${ids(i)}, data: ${scatter_buff(i).map(_.mkString(",")).mkString(" \\ ")}");
		}
		
	}
};