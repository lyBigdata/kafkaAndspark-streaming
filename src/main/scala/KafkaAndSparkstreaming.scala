import java.lang.StringBuffer

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{streaming, SparkContext, SparkConf}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.File

/**
 * Created by liuyu on 15-8-29.
 */

/*
# ####The log files that we use for this assignment are in the [Apache Common Log Format (CLF)](http://httpd.apache.org/docs/1.3/logs.html#common). The log file entries produced in CLF will look something like this:
# `127.0.0.1 - - [01/Aug/1995:00:00:01 -0400] "GET /images/launch-logo.gif HTTP/1.0" 200 1839`
#
# ####Each part of this log entry is described below.
# * `127.0.0.1`
# ####This is the IP address (or host name, if available) of the client (remote host) which made the request to the server.
#
# * `-`
# ####The "hyphen" in the output indicates that the requested piece of information (user identity from remote machine) is not available.
#
# * `-`
# ####The "hyphen" in the output indicates that the requested piece of information (user identity from local logon) is not available.
#
# * `[01/Aug/1995:00:00:01 -0400]`
# ####The time that the server finished processing the request. The format is:
# `[day/month/year:hour:minute:second timezone]`
#   * ####day = 2 digits
#   * ####month = 3 letters
#   * ####year = 4 digits
#   * ####hour = 2 digits
#   * ####minute = 2 digits
#   * ####second = 2 digits
#   * ####zone = (\+ | \-) 4 digits
#
# * `"GET /images/launch-logo.gif HTTP/1.0"`
# ####This is the first line of the request string from the client. It consists of a three components: the request method (e.g., `GET`, `POST`, etc.), the endpoint (a [Uniform Resource Identifier](http://en.wikipedia.org/wiki/Uniform_resource_identifier)), and the client protocol version.
#
# * `200`
# ####This is the status code that the server sends back to the client. This information is very valuable, because it reveals whether the request resulted in a successful response (codes beginning in 2), a redirection (codes beginning in 3), an error caused by the client (codes beginning in 4), or an error in the server (codes beginning in 5). The full list of possible status codes can be found in the HTTP specification ([RFC 2616](https://www.ietf.org/rfc/rfc2616.txt) section 10).
#
# * `1839`
# ####The last entry indicates the size of the object returned to the client, not including the response headers. If no content was returned to the client, this value will be "-" (or sometimes 0).
#
# ####Note that log files contain information supplied directly by the client, without escaping. Therefore, it is possible for malicious clients to insert control-characters in the log files, *so care must be taken in dealing with raw logs.*

*/

//定义样例类，模式匹配日志文件 :127.0.0.1 - - [01/Aug/1995:00:00:01 -0400] "GET /images/launch-logo.gif HTTP/1.0" 200 1839
case class Apachelogs(host :String, usereinfo:String,userid:String,time :String,requests :String,status :String ,bytestotal:String)
case class parseApachelogs(host:String,userinfo:String,userid:String,time :String,method:String,resource:String,protol:String,status:String,bytestotal:String)
case class matchInfo(host:String,userid:String,bytestotal :String)

object KafkaAndSparkstreaming {

  //定义正则表达式，用来匹配日志
  final val APACHE_ACCESS_LOG_PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)""".r
  //月份映射
  val monthMap=HashMap(("Jan",1),("Feb",2),("Mar",3),("Apr",4),("May",5),("Jun",6),("Jul",7),("Aug",8),("Sep",9),("Oct",10),("Nov",11),("Dec",12))

  //解析时间
  def parseLogsTime(time:String):String= {
    var dadeTime :String=""
    val dateTime = time.substring(1, time.indexOf(':'))
    val timeArray = dateTime.split( """/""")
    val day = timeArray(0)
    val month=monthMap.get(timeArray(1))
    val year=timeArray(2)

    dadeTime.concat(year+"/"+month.toString+"/"+day)
  }

  //解析请求信息
  def parseRequests(requests:String):(String,String,String)={
    val splitRequests=requests.substring(1,requests.lastIndexOf('"')).split("""\s+""")
    val method=splitRequests(0)
    val resource=splitRequests(1)
    val protol=splitRequests(2)

    (method,resource,protol)
  }

  //解析日志
  def parseLogs(everyLines :String):String={

    val parseString=""

    val logsArray=everyLines.split("""\s+""")

    val host=logsArray(0)
    val userinfo=logsArray(1)
    val userid=logsArray(2)
    val parseAfterTime=parseLogsTime(logsArray(3))
    val (method,resource,protol)=parseRequests(logsArray(4))
    val status=logsArray(5)
    val bytesTotal=logsArray(6)

    val afterString=host+"\t"+userinfo+"\t"+userid+"\t"+parseAfterTime+"\t"+method+"\t"+resource+"\t"+protol+"\t"+status+"\t"+bytesTotal
    parseString.concat(afterString)
  }

  def mian(args:Array[String]): Unit ={

    //判断主函数带参数是否与所需的相同
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads) = args

    val sparkconf=new SparkConf().setAppName("KafkaAndSparkstreaming").setMaster("local[2]")

    val sparkcontext=new SparkContext(sparkconf)

    val streamingcontext=new StreamingContext(sparkcontext,streaming.Seconds(1))

    streamingcontext.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val kafkaStream = KafkaUtils.createStream(streamingcontext, zkQuorum, group, topicMap).map(_._2)


    var dataDsream=kafkaStream.filter(_.split("\\s+").length==7).map(line=>(line(0),line(4),line(5))).cache()

    dataDsream.saveAsTextFiles("hdfs://master:9000/dataOut")

    //统计同一IP的访问次数
    val IP=dataDsream.map(yuanzu=>(yuanzu._1,(yuanzu._2,yuanzu._3))).map((_,1)).reduceByKey(_+_)

    IP.print(10)

    //统计访问成功的次数
    val success=dataDsream.filter(yuan=>yuan._3=="200").count()

    //统计出现404错的相关信息

    val error404=dataDsream.filter(zu=>zu._3=="404").map(info=>(info._1,info._2)).print()

    //统计访问量最多的资源
    val manyResource=dataDsream.map{l=>
                (File(l._2.toString).name,1)}.reduceByKey(_+_).print(10)

    //System.out.println("KafkaAndSparkstreaming")


    kafkaStream.count()

    //解析日志文件
   val dataStream= kafkaStream.filter(lines=>APACHE_ACCESS_LOG_PATTERN.findAllMatchIn(lines.toString)!=Nil).map(ll=>parseLogs(ll)).cache()
    dataStream.saveAsTextFiles("hdfs://master:9000/ApacheLogs/parseLogsOut/filterOut")

    dataStream.count()  //统计过滤解析后的日志文件数据



    //计算内容传输量的平均值，最大和最小
    val content_size=dataStream.map(_.split("""\t""")).filter(_.length==9).map{lines=>
                                            lines(8)
                                            }.cache()
    val averageContent_size={content_size.reduce((lift,right)=>(lift.toInt+right.toInt).toString)}.toString.toInt  / (content_size.count().toString.toInt)

    println("Content Size average :"+averageContent_size)

    var minContent_size=Integer.MAX_VALUE
    content_size.map(size=>
                        {if(size.toInt<minContent_size)
                          minContent_size=size.toInt
                        }
                      )
    println("Content Size min :"+minContent_size)

    var maxContent_size=Integer.MIN_VALUE
    content_size.map(size=>
                        { if(size.toInt > maxContent_size)
                          maxContent_size=size.toInt

                        })

    println("Content Size max :"+minContent_size)



    //统计独立访问IP数
    val uniqueIp=dataStream.map(_.split("""\t""")).filter(_.length==9).map{lines=>(lines(0),1)}.reduceByKey(_+_)
    val Ipcount=uniqueIp.count()
    uniqueIp.print()

  }
}
