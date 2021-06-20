package com.sparkTutorial.rdd.nasaApacheWebLogs

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UnionLogProblem {

  def main(args: Array[String]) {

    /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
       "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
       Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
       take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.

       Keep in mind, that the original log files contains the following header lines.
       host	logname	time	method	url	response	bytes

       Make sure the head lines are removed in the resulting RDD.
     */
    val conf = new SparkConf().setAppName("nasa").setMaster("local[*]").set("spark.testing.memory", "536870912")
    val sc = new SparkContext(conf)

    val julyLog: RDD[String] = sc.textFile("in/nasa_19950701.tsv")
    val augLog: RDD[String] = sc.textFile("in/nasa_19950801.tsv")
    val bothLog = julyLog.union(augLog)

    val header = "host\tlogname\ttime\tmethod\turl\tresponse\tbytes"
    val cleanedBothLog = bothLog.filter(_ != header)
    val sampledLog = cleanedBothLog.sample(withReplacement = true, fraction = 0.1)
    sampledLog.saveAsTextFile("out/sample_nasa_logs.tsv")
  }
}
