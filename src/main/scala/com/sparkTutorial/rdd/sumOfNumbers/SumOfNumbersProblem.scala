package com.sparkTutorial.rdd.sumOfNumbers

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SumOfNumbersProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
       print the sum of those numbers to console.

       Each row of the input file contains 10 prime numbers separated by spaces.
     */
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("sumOfNumbers").setMaster("local[1]").set("spark.testing.memory", "536870912")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("in/prime_nums.text")
    val numStrs: RDD[String] = lines.flatMap(line => line.split("\\s+"))
    val numbers: RDD[Int] = numStrs.filter(!_.isEmpty).map(str => str.toInt)
    val sum: Int = numbers.reduce((x: Int, y: Int) => x + y)
    println("Sum is: " + sum)

  }
}
