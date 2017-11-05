# Akka-AllReduce
The code is in "akka/src/main/cluster/sample/cluster/AllReduce/"

Run Master: sbt "runMain sample.cluster.AllReduce.AllReduceMaster" "

Run Worker: sbt "runMain sample.cluster.AllReduce.AllReduceWorker $PORT" "

Based on GridWorker and Akka-sample.