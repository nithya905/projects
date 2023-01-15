from pyspark.sql import SparkSession
import os

spark_session=SparkSession.builder.master("local[*]").appName("Confluent").getOrCreate()

confluentClusterName = "ineuron"
confluentBootstrapServers = "pkc-6ojv2.us-west4.gcp.confluent.cloud:9092"
confluentTopicName = "spark_topic"
confluentApiKey = "BVB5KR2VU6FJM2U6"
confluentSecret = "W2mF2SFLePJrLJ7yFK92UnB58mv3JPw4wGF0AgILZqTPZVMRBXFoaP5kIUeafHZ+"

df = (spark_session
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", confluentBootstrapServers)
          .option("kafka.security.protocol", "SASL_SSL")
          .option("kafka.sasl.jaas.config",
                  "org.apache.kafka.common.security.plain.PlainLoginModule  required username='{}' password='{}';".format(confluentApiKey, confluentSecret))
          .option("kafka.ssl.endpoint.identification.algorithm", "https")
          .option("kafka.sasl.mechanism", "PLAIN")
          .option("subscribe", confluentTopicName)
          .option("startingOffsets", "earliest")
          .option("failOnDataLoss", "false")
          .load()
          )

df = (df.withColumn('key_str',df['key'].cast('string').alias('key_str')).drop('key').withColumn('value_str',df['value'].cast('string').alias('key_str')))


query = (df.writeStream
             .format("csv")
             .option("format", "append")
             .trigger(processingTime="5 seconds")
             .option("checkpointLocation", os.path.join("csv_checkpoint"))
             .option("path", os.path.join("csv"))
             .outputMode("append")
             .start()
             )


query = (df.writeStream
             .format("kafka")
             .option("kafka.bootstrap.servers", confluentBootstrapServers)
             .option("kafka.security.protocol", "SASL_SSL")
             .option("kafka.sasl.jaas.config",
                     "org.apache.kafka.common.security.plain.PlainLoginModule  required username='{}' password='{}';".format(
                         confluentApiKey, confluentSecret))
             .option("kafka.ssl.endpoint.identification.algorithm", "https")
             .option("kafka.sasl.mechanism", "PLAIN")
             .option("checkpointLocation", os.path.join("kafka_checkpoint"))
             .option("topic", confluentTopicName).start())
query.awaitTermination()             