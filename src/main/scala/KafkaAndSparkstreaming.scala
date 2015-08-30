import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{streaming, SparkContext, SparkConf}

import scala.reflect.io.File

/**
 * Created by liuyu on 15-8-29.
 */

//定义样例类，模式匹配日志文件
case class Apachelogs(hostname :String, useremail:String,username:String,time :String,requests :String,status :String ,bytestotal:String)

object KafkaAndSparkstreaming {

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

    val dataDsream=kafkaStream.filter(_.split("\\s+").length==7).map(line=>(line(0),line(4),line(5))).cache()

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

}
}
