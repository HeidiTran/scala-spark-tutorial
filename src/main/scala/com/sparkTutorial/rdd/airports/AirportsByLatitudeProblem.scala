package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AirportsByLatitudeProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
       Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "St Anthony", 51.391944
       "Tofino", 49.082222
       ...
     */

    val conf = new SparkConf().setAppName("airports").setMaster("local[2]").set("spark.testing.memory", "536870912")
    val sc = new SparkContext(conf)
    val airports: RDD[String] = sc.textFile("in/airports.text")
    val airportsWithLargeLatitude: RDD[String] = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(6).toDouble > 40)

    val airportsNamesAndLatitude: RDD[String] = airportsWithLargeLatitude.map(airport => {
      val fields = airport.split(Utils.COMMA_DELIMITER)
      fields(1) + ", " + fields(6)
    })

    airportsNamesAndLatitude.saveAsTextFile("out/airports_by_latitude.text")
  }
}
