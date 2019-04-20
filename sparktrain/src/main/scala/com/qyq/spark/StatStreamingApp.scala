package com.qyq.spark

import com.qyq.domain.ClickLog
import kafka.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StatStreamingApp {
  def main(args: Array[String]): Unit = {
    if(args.length != 4){
      println("Usage: StatStreamingApp <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    val Array(zkQuorum, groupId, topics, numThreads) = args

    val sparkConf = new SparkConf().setAppName("StatStreamingApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf , Seconds(60))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val messages = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)

    //测试步骤一：测试数据接收
//    messages.map(_._2).count().print

    //测试步骤二：数据清洗
    val logs = messages.map(_._2)
    val cleanData = logs.map(line =>{
      val infos = line.split("\t")

      val url = infos(2).split(" ")(1)
      var courseId = 0

      //把实战课程的课程编号拿到了
      if(url.startsWith("/class")){
        val courseIdHTML = url.split("/")(2)
        courseId = courseIdHTML.substring(0, courseIdHTML.lastIndexOf(".")).toInt
      }
      ClickLog(infos(0), DateUtils.parseToMinute(infos(1)), courseId, infos(3).toInt, infos(4))
    }).filter(clicklog => clicklog.courseId != 0)

    cleanData.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
