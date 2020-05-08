import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.BufferedSource

/**
  * @ author yulong
  * @ createTime 2020-04-08 11:59
  */
object KafkaProducer {

  def main(args: Array[String]): Unit = {

    writeToKafka("hotitemscount")

  }

  def writeToKafka(topic:String)={

    val properties = new Properties()
    properties.setProperty("bootstrap.servers","hadoop102:9092")
    properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("auto.offset.reset","latest")

    val producer = new KafkaProducer[String, String](properties)

    val source: BufferedSource = io.Source.fromFile("D:\\Mywork\\workspace\\IdeaProjects\\baway2019\\UserBehaviorAnalysis-1707E\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    //一行一行的发数据
    for (elem <- source.getLines()) {
      //发数据得先建ProducerRecord
      val value = new ProducerRecord[String, String](topic,elem)
      producer.send(value)
    }

    producer.close()


  }



}
