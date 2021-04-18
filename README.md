# SparkKafka

In this project i have shown how to integrated Kafka with Spark . I have made use of spark with scala to have a consumer API and display the processed output in Spark console.Kafka is a potential messaging and integration platform for Spark streaming. Kafka act as the central hub for real-time streams of data and are processed using complex algorithms in Spark Streaming.

LocationStrategies

The new Kafka consumer API will pre-fetch messages into buffers. Therefore it is important for performance reasons that the Spark integration keep cached consumers on executors (rather than recreating them for each batch), and prefer to schedule partitions on the host locations that have the appropriate consumers.

In most cases, we should use LocationStrategies.PreferConsistent as shown above. This will distribute partitions evenly across available executors. If our executors are on the same hosts as our Kafka brokers, use PreferBrokers, which will prefer to schedule partitions on the Kafka leader for that partition. Finally, if we have a significant skew in load among partitions, use PreferFixed. This allows us to specify an explicit mapping of partitions to hosts (any unspecified partitions will use a consistent location).

The cache for consumers has a default maximum size of 64. If we expect to be handling more than (64 \* number of executors) Kafka partitions, we can change this setting via spark.streaming.kafka.consumer.cache.maxCapacity.

If we would like to disable the caching for Kafka consumers, we can set spark.streaming.kafka.consumer.cache.enabled to false. Disabling the cache may be needed to workaround the problem described in SPARK-19185. This property may be removed in later versions of Spark, once SPARK-19185 is resolved.

ConsumerStrategies

The new Kafka consumer API has a number of different ways to specify topics, some of which require considerable post-object-instantiation setup. ConsumerStrategies provides an abstraction that allows Spark to obtain properly configured consumers even after restart from checkpoint.

ConsumerStrategies.Subscribe, as shown above, allows us to subscribe to a fixed collection of topics. SubscribePattern allows us to use a regex to specify topics of interest. Note that unlike the 0.8 integration, using Subscribe or SubscribePattern should respond to adding partitions during a running stream. Finally, Assign allows us to specify a fixed collection of partitions. All three strategies have overloaded constructors that allow us to specify the starting offset for a particular partition.

Steps Included :-

1.  Start the Zookeeper Service

                        bin/zookeeper-server-start.sh config/zookeeper.properties

2.  Start the Kafka Server

                       bin/kafka-server-start.sh config/server.properties

3.Initiate the kafka producer with the topic

                       bin/kafka-console-producer.sh --broker-list localhost:9092 --topic messagetopic

My code here takes the message as an input and print outs the words in the message along with the word count.

# Sample Output

![alt text](https://github.com/Abhishek010397/SparkKafka/blob/main/KafkaSpark.png)
