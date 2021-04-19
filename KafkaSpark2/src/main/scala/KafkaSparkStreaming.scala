import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaSparkStreaming {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val broker_id = "localhost:9092"
    val group_id = "GRP2"
    val topics = "kafkatopic"
    val topic_set = topics.split(",").toSet

    val kafkaParams = Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_id,
      ConsumerConfig.GROUP_ID_CONFIG -> group_id,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    println("Program Started")

    val conf = new SparkConf().setMaster("local").setAppName("kafkaSpark")

    val ssc = new StreamingContext(conf,Seconds(5))

    val sc = ssc.sparkContext
    sc.setLogLevel("OFF")

    val kafkaSpark = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topic_set,kafkaParams)
    )

    kafkaSpark.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
