package com.classcat

import akka.actor.Actor
import spray.routing._
import spray.http._
import MediaTypes._

import org.apache.spark._
import org.apache.spark.SparkContext._

import org.joda.time.Instant
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat

import com.classcat.ccnsm.{DataMain, ViewMain}

class MyConf {
    val ip = "192.168.0.50"
}

// we don't implement our route structure directly in the service actor because
// we want to be able to test it independently, without having to spin up an actor
class CCNSMServiceActor extends Actor with defaultService with MyService {
    println("In MyServiceActor\n")

  // the HttpService trait defines only one abstract member, which
  // connects the services environment to the enclosing actor or test
  def actorRefFactory = context

  // this actor only runs our route, but you could add
  // other things here, like request stream processing
  // or timeout handling
  def receive = runRoute(defaultRoute ~ myRoute)
}


// this trait defines our service behavior independently from the service actor
trait defaultService extends HttpService {
    println("gaugau")
    val a = "uriuri"
    val conf = new SparkConf().setMaster("local").setAppName("cc-nsm")
    val sc = new SparkContext(conf)


    val defaultRoute =
        pathSingleSlash {
            redirect("/main", StatusCodes.Found)
        } ~
        pathPrefix("ccimg") {
            getFromResourceDirectory("ccimg")
        }~
        pathPrefix("css") {
            getFromResourceDirectory("css")
        }~
        pathPrefix("img") {
            getFromResourceDirectory("img")
        }~
        pathPrefix("js") {
            getFromResourceDirectory("js")
        }~
        path("main") {
            get {
                respondWithMediaType(`text/html`) {
                    complete {
                        val data_capsule = new DataMain(sc)
                        val rdd_tcp_incoming = data_capsule.getRddTcpIncoming
                        val rdd_tcp_outgoing = data_capsule.getRddTcpOutgoing

                        val view = new ViewMain(rdd_tcp_incoming, rdd_tcp_outgoing)
                        val buffer = view.getHtml

                        val meta_refresh = """<meta http-equiv="refresh" content="90" />"""
                        html.view.render(meta_refresh, buffer).body
                    }
                }
            }
        } ~
      path("") {
          // val lines = sc.textFile("file:///usr/local/bro/logs/current/conn.log")
          // lines.collect.foreach(println)

        get {

          respondWithMediaType(`text/html`) { // XML is marshalled to `text/xml` by default, so we simply override here
              print ("にゃにゃう")
            complete {
              <html>
                <body>
                  <h1>Say Default to <i>spray-routing</i> on <i>spray-can</i>!</h1>
                </body>
              </html>
            }
          }
        }
    } ~
    path("main2") {
      get {
        respondWithMediaType(`text/html`) { // XML is marshalled to `text/xml` by default, so we simply override here
            print("テンプレート")
          complete {
              print ("へいへい")
              var buffer = ""
              val row_data = sc.textFile("file:///usr/local/bro/logs/current/conn.log")
              val data = row_data.filter(! _.startsWith("#"))

              // Split TSV to get tokens.
              val dataset = data.map(line => line.split("\t").map(elem => elem.trim))

              //  fields ts(0)     uid(1)     id.orig_h(2)       id.orig_p(3)       id.resp_h(4)      id.resp_p(5)      proto(6)   service duration        orig_bytes      resp_bytes      conn_state      local_orig      local_resp      missed_bytes    history orig_pkts       orig_ip_bytes   resp_pkts       resp_ip_bytes   tunnel_parents
              // types  time    string  addr    port    addr    port    enum    string  interval        count   count   string  bool    bool    count   string  count   count   count   count   set[string]
              val dataset_tcp = dataset.filter(_(6) == "tcp").sortBy( { x => x(0) }, false)
              //               val dataset_tcp = dataset.filter(_(6) == "tcp").sortBy({x => x(0)})

              val dataset_tcp_incoming = dataset_tcp.filter( { x => val myconf = new com.classcat.MyConf(); x(4) == myconf.ip } )
              //               val dataset_tcp_incoming = dataset_tcp.filter( { x => x(4) == "192.168.0.50" } )

              //               val dataset_tcp_incoming = dataset_tcp.filter(_(4) == myip)


              // dataset_tcp_incoming.collect.foreach({ tokens => print(tokens(6))})
              buffer += "<table>"

              /* data.collect.foreach(
                  {
                      i => println(i)
                      buffer = buffer + i
                  }
              ) */
              dataset_tcp_incoming.collect.foreach( {
                  tokens =>
                  val ts = tokens(0)
                  val ts2 = ts.toDouble*1000L
                  var i = new Instant(ts2.longValue)
                  val dt = i.toDateTime()
                  // val dt = new DateTime(new Instant(ts2.intValue)) // , DateTimeZone.UTC)
                  // val dt = new DateTime(ts.toDouble.intValue) //, DateTimeZone.UTC)
                  // val fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss")
                  buffer += "<tr>"
                  buffer += "<td>" + tokens(0)
                  buffer += "<td>" + dt.toString()
                  buffer += "<td>" + tokens(1)
                  buffer += "<td>" + tokens(2)
                  buffer += "<td>" + tokens(3)
                  buffer += "<td>" + tokens(4)
                  buffer += "<td>" + tokens(5)
                  buffer += "<td>" + tokens(6)
                  })
              buffer += "</table>"


              html.index.render("にゃおちゃん" + buffer).body
          }
        }
      }
  }

}

trait MyService extends HttpService {

  val myRoute =
    path("hello") {
      get {
        respondWithMediaType(`text/html`) { // XML is marshalled to `text/xml` by default, so we simply override here
          complete {
            <html>
              <body>
                <h1>Say hello to <i>spray-routing</i> on <i>spray-can</i>!</h1>
              </body>
            </html>
          }
        }
      }
  } ~
    path("world") {
      get {
        respondWithMediaType(`text/html`) { // XML is marshalled to `text/xml` by default, so we simply override here
          complete {
            <html>
              <body>
                <h1>Say World to <i>spray-routing</i> on <i>spray-can</i>!</h1>
              </body>
            </html>
          }
        }
      }
    }

}
