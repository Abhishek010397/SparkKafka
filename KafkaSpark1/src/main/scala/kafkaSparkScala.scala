import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object kafkaSparkScala {

  def main(args: Array[String]): Unit ={

    val broker_id = "localhost:9092"
    val group_id = "GRP1"
    val topics = "messagetopic"
    //If there are multiple topics in a single topic,convert it to a set
    val topic_set = topics.split(",").toSet

    val kafkaParams = Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_id,
      ConsumerConfig.GROUP_ID_CONFIG -> group_id,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val sparkconf =  new SparkConf().setMaster("local").setAppName("KafkaSpark")

    //2nd param here would be seconds so that SparkContext will look at the producer and display word count and msgs in the consumer console
    val StreamingContext = new StreamingContext(sparkconf,Seconds(5))

    val sc = StreamingContext.sparkContext

    //Switch Off getting Logs in Console
    sc.setLogLevel("OFF")



    //Integrate kafkaParams And the StreamingContext

    //Message Structure,DirectStreams would be of Strings
    val message = KafkaUtils.createDirectStream[String,String](
      StreamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topic_set,kafkaParams)
    )

    //calculate word Count from our Message using flatMap and split with space b/w words
    val words = message.map(_.value()).flatMap(_.split(" "))

    //Get the count
    val count_words = words.map(x=>(x,1)).reduceByKey(_+_)

    count_words.print()

    //Initiate sparkStreaming
    StreamingContext.start()
    StreamingContext.awaitTermination()

  }

}
