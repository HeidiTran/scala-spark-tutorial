package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AirportsInUsaProblem {
  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
       and output the airport's name and the city's name to out/airports_in_usa.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "Putnam County Airport", "Greencastle"
       "Dowagiac Municipal Airport", "Dowagiac"
       ...
     */

    val conf = new SparkConf().setAppName("airports").setMaster("local[2]").set("spark.testing.memory", "536870912")
    val sc = new SparkContext(conf)

    val airports: RDD[String] = sc.textFile("in/airports.text") // Each line is a String
    val airportsInUSA: RDD[String] = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(3) == "\"United States\"") // index (3) is Country where airport is located

    val airportsNameAndCityNames: RDD[String] = airportsInUSA.map(line => {
      val fields = line.split(Utils.COMMA_DELIMITER)
      fields(1) + ", " + fields(2)  // Get airport's name and the city's name
    })

    /* Since we've specified running the app on 2 threads -> there'll be 2 output files
     * Under out/airports_in_usa.text folder, they're part-00000 and part-00001
     * Each file corresponds to the result from a thread
     */
    airportsNameAndCityNames.saveAsTextFile("out/airports_in_usa.text")
  }
}
