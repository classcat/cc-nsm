
package com.classcat.ccnsm

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row;
// Import Spark SQL data types
import org.apache.spark.sql.types.{StructType,StructField,StringType,FloatType,DoubleType};

import org.joda.time.Instant
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat

import com.classcat.MyConf


/*
#  fields ts(0)     uid(1)     id.orig_h(2)       id.orig_p(3)       id.resp_h(4)      id.resp_p(5)      proto(6)   service duration        orig_bytes      resp_bytes      conn_state      local_orig      local_resp      missed_bytes    history orig_pkts       orig_ip_bytes   resp_pkts       resp_ip_bytes   tunnel_parents
# types  time    string  addr    port    addr    port    enum    string  interval        count   count   string  bool    bool    count   string  count   count   count   count   set[string]
*/

/*
class MyConf {
    val ip = "192.168.0.50"
} */

class DataMain2 (sc : org.apache.spark.SparkContext) {
    println("I'm datamain2 constructor")
    // sc is an existing SparkContext.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val rdd_raw = sc.textFile("file:///usr/local/bro/logs/current/conn.log")

    val rdd = rdd_raw.filter(! _.startsWith("#"))

    val rowRDD = rdd.map(_.split("\t")).map(rec => Row(rec(0).trim().toDouble, rec(2), rec(3), rec(4), rec(5), rec(6)))
    // Convert records of the RDD (people) to Rows.
    //val rowRDD = rdd.map(_.split(",")).map(p => Row(p(0), p(1).trim))

    // Split TSV to get tokens.
    //val dataset = data.map(line => line.split("\t").map(elem => elem.trim))

    // The schema is encoded in a string
    val schemaString = "ts orig_h orig_p resp_h resp_p proto"

    // Generate the schema based on the string of schema
    //val schema =
    //  StructType(
    //    schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    val schema = StructType (
        List(
            StructField("ts", DoubleType, false),
            StructField("orig_h", StringType, false),
            StructField("orig_p", StringType, false),
            StructField("resp_h", StringType, false),
            StructField("resp_p", StringType, false),
            StructField("proto", StringType, false)
        )
    )

    // Apply the schema to the RDD.
    val dataFrame = sqlContext.createDataFrame(rowRDD, schema)

    // Register the DataFrames as a table.
    dataFrame.registerTempTable("curr_conn")

    val results_tcp = sqlContext.sql("SELECT * FROM curr_conn WHERE proto='tcp'").toDF()
    results_tcp.sort("ts").foreach (
        x => {
            val ts = x(0).toString
            val ts2 = ts.toDouble*1000L
            var i = new Instant(ts2.longValue)
            val dt = i.toDateTime()
            println(dt.toString("HH:mm:ss.SSS MM/dd"))
        }
    )


/*    dataFrame.sortBy("ts", false).foreach(
        {
            x => println(x(0))
        }
    )
*/

    // dataFrame.printSchema()

    /*
    root
     |-- ts: double (nullable = false)
     |-- orig_h: string (nullable = false)
     |-- orig_p: string (nullable = false)
     |-- resp_h: string (nullable = false)
     |-- resp_p: string (nullable = false)
     |-- proto: string (nullable = false)
     */

     // dataFrame.select("ts").show()

    // SQL statements can be run by using the sql methods provided by sqlContext.
    // val results = sqlContext.sql("SELECT name FROM people")
    /*
    val results = sqlContext.sql("SELECT proto FROM curr_conn")

    val results_tcp = sqlContext.sql("SELECT * FROM curr_conn WHERE proto='tcp' order by 'ts' DESC limit 20")
    results_tcp.foreach(
        x => {
            val ts = x(0).toString
            val ts2 = ts.toDouble*1000L
            var i = new Instant(ts2.longValue)
            val dt = i.toDateTime()
            println(dt.toString("HH:mm:ss.SSS MM/dd"))
        }
    ) */

    // results.map(t => "Proto: " + t(0)).collect().foreach(println)

    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by field index or by field name.
    // results.map(t => "Name: " + t(0)).collect().foreach(println)
}
