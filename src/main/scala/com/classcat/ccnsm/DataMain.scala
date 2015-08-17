
package com.classcat.ccnsm

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import org.apache.spark.sql.Row;

import com.classcat.MyConf

/*
#  fields ts(0)     uid(1)     id.orig_h(2)       id.orig_p(3)       id.resp_h(4)      id.resp_p(5)      proto(6)   service duration        orig_bytes      resp_bytes      conn_state      local_orig      local_resp      missed_bytes    history orig_pkts       orig_ip_bytes   resp_pkts       resp_ip_bytes   tunnel_parents
# types  time    string  addr    port    addr    port    enum    string  interval        count   count   string  bool    bool    count   string  count   count   count   count   set[string]
*/

/*
class MyConf {
    val ip = "192.168.0.50"
} */

class DataMain (sc : org.apache.spark.SparkContext) {
    private var is_error = false
    private var msg_error = ""

    private var rdd_tcp_incoming : RDD[Array[String]] = _ // null
    private var rdd_tcp_outgoing : RDD[Array[String]] = _
    private var rdd_tcp_others : RDD[Array[String]] = _

    private var rdd_tcp_incoming_group_by_orig_h : RDD[(String, Int)] = _
    private var rdd_tcp_outgoing_group_by_resp_h : RDD[(String, Int)] = _

    private var rdd_tcp_incoming_group_by_resp_p : RDD[(String, Int)] = _
    private var rdd_tcp_outgoing_group_by_resp_p : RDD[(String, Int)] = _

    try {
        val row_data = sc.textFile("file:///usr/local/bro/logs/current/conn.log").cache()

        val dataset = row_data.filter(! _.startsWith("#")).map(_.split("\t"))

        val dataset_tcp = dataset.filter(_(6) == "tcp").sortBy( { x => x(0) }, false)

        rdd_tcp_incoming = dataset_tcp.filter( { x => val myconf = new MyConf(); x(4) == myconf.ip } )

        rdd_tcp_outgoing = dataset_tcp.filter( { x => val myconf = new MyConf(); x(2) == myconf.ip } )

        rdd_tcp_others = dataset_tcp.filter( { x => val myconf = new MyConf(); (x(2) != myconf.ip) && (x(4) != myconf.ip) } )

        /*
        rdd_tcp_outgoing.groupBy({x => x(4)}).collect.foreach(
            x =>
            {
                println(x._1)
                println(x._2.toArray.length)
            }
        ) */

        rdd_tcp_incoming_group_by_orig_h = rdd_tcp_incoming.groupBy({ x => x(2)}).map( x => {(x._1, x._2.toArray.length)}).sortBy( { x => x._2 }, false)


        rdd_tcp_outgoing_group_by_resp_h = rdd_tcp_outgoing.groupBy({ x => x(4)}).map( x => {(x._1, x._2.toArray.length)}).sortBy( { x => x._2 }, false)

        rdd_tcp_incoming_group_by_resp_p = rdd_tcp_incoming.groupBy({ x => x(5)}).map( x => {(x._1, x._2.toArray.length)}).sortBy( { x => x._2 }, false)

        rdd_tcp_outgoing_group_by_resp_p = rdd_tcp_outgoing.groupBy({ x => x(5)}).map( x => {(x._1, x._2.toArray.length)}).sortBy( { x => x._2 }, false)
        /* rdd_tcp_outgoing_group_by_resp_h.collect.foreach(
            x =>
            {
                println(x._1)
                println(x._2)
            }
        ) */
        // val rdd_tmp = rdd_tcp_outgoing_group_by_resp_h.sortBy( { x => x._2 }, false)
        /* rdd_tcp_outgoing_group_by_resp_h.collect.foreach(
            x =>
            {
                println(x._1)
                println(x._2)
            }
        ) */

    } catch {
        case e:Exception => {
            is_error = true
            msg_error = e.toString
        }
    }

    def isError () : (Boolean, String) = {
        return (is_error, msg_error)
    }

    def getRddTcpIncoming () : RDD[Array[String]] = {
        return rdd_tcp_incoming
    }

    def getRddTcpOutgoing () : RDD[Array[String]] = {
        return rdd_tcp_outgoing
    }

    def getRddTcpOthers () : RDD[Array[String]] = {
        return rdd_tcp_others
    }

    def getRddTcpIncomingGroupByOrigH () : RDD[(String, Int)] = {
        return rdd_tcp_incoming_group_by_orig_h
    }

    def getRddTcpOutgoingGroupByRespH () : RDD[(String, Int)] = {
        return rdd_tcp_outgoing_group_by_resp_h
    }

    def getRddTcpIncomingGroupByRespP () : RDD[(String, Int)] = {
        return rdd_tcp_incoming_group_by_resp_p
    }

    def getRddTcpOutgoingGroupByRespP () : RDD[(String, Int)] = {
        return rdd_tcp_outgoing_group_by_resp_p
    }

}
