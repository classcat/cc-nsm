
package com.classcat.ccnsm

// import org.apache.spark._
import org.apache.spark.rdd.RDD
// import org.apache.spark.SparkContext._

import org.joda.time.Instant
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat

class ViewMain (rdd_tcp_incoming : RDD[Array[String]], rdd_rcp_outgoing : RDD[Array[String]] ) {
    private var buffer : String = ""

    tcp

    def tcp = {
        buffer += """<table>"""
        buffer += """<tr><td width="50%">"""

        buffer += tcp_incoming_latest

        buffer += "<br/>"

        buffer += tcp_outgoing_latest
        // buffer += "left pane"

        buffer += """<td width="50%">"""

        // buffer += "right pane"

        buffer += """</table>"""
    }

    def tcp_incoming_latest : String = {
        var lbuffer : String = ""

        lbuffer += "<table>"
        lbuffer += "<caption><strong>最新の TCP 接続 (incoming)</strong></caption>"
        lbuffer += "<tr><th><th>タイムスタンプ<th>接続元<th>ポート<th>接続先<th>ポート<th>プロトコル</tr>"

        val rdd_with_index = rdd_tcp_incoming.zipWithIndex

        rdd_with_index.take(50).foreach {
        // rdd_with_index.collect.foreach {
            x => {
                // (Array[String], Long)
                val tokens = x._1
                val index = x._2

                val ts = tokens(0)
                val ts2 = ts.toDouble*1000L
                var i = new Instant(ts2.longValue)
                val dt = i.toDateTime()

                lbuffer += "<tr>"
                lbuffer += "<td>" + (index+1).toString
                lbuffer += "<td>" + dt.toString("HH:mm:ss.SSS MM/dd")

                // lbuffer += "<td>" + tokens(1)
                lbuffer += "<td>" + tokens(2)
                lbuffer += "<td>" + tokens(3)
                lbuffer += "<td>" + tokens(4)
                lbuffer += "<td>" + tokens(5)
                lbuffer += """<td align="center">""" + tokens(6) // protocol
            }
        }

        lbuffer += "</table>\n"

        return lbuffer
    }

    def tcp_outgoing_latest : String = {
        var lbuffer : String = ""

        lbuffer += "<table>"
        lbuffer += "<caption><strong>最新の TCP 接続 (outgoing)</strong></caption>"
        lbuffer += "<tr><th><th>タイムスタンプ<th>接続元<th>ポート<th>接続先<th>ポート<th>プロトコル</tr>"

        val rdd_with_index = rdd_rcp_outgoing.zipWithIndex

        rdd_with_index.take(50).foreach {
        // rdd_with_index.collect.foreach {
            x => {
                // (Array[String], Long)
                val tokens = x._1
                val index = x._2

                val ts = tokens(0)
                val ts2 = ts.toDouble*1000L
                var i = new Instant(ts2.longValue)
                val dt = i.toDateTime()

                lbuffer += "<tr>"
                lbuffer += "<td>" + (index+1).toString
                lbuffer += "<td>" + dt.toString("HH:mm:ss.SSS MM/dd")
                // lbuffer += "<td>" + tokens(1)
                lbuffer += "<td>" + tokens(2)
                lbuffer += "<td>" + tokens(3)
                lbuffer += "<td>" + tokens(4)
                lbuffer += "<td>" + tokens(5)
                lbuffer += """<td align="center">""" + tokens(6) // protocol
            }
        }

        /*
        rdd_rcp_outgoing.collect.foreach {
            tokens => {
                val ts = tokens(0)
                val ts2 = ts.toDouble*1000L
                var i = new Instant(ts2.longValue)
                val dt = i.toDateTime()

                lbuffer += "<tr>"
                lbuffer += "<td>" + dt.toString
                lbuffer += "<td>" + tokens(1)
                lbuffer += "<td>" + tokens(2)
                lbuffer += "<td>" + tokens(3)
                lbuffer += "<td>" + tokens(4)
                lbuffer += "<td>" + tokens(5)
                lbuffer += "<td>" + tokens(6) // protocol
            }
        } */

        lbuffer += "</table>\n"

        return lbuffer
    }

    def getHtml : String = {
        return buffer
    }
}
