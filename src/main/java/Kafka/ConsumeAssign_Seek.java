package Kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumeAssign_Seek {
    static Logger logger = LoggerFactory.getLogger(ConsumeAssign_Seek.class);

    public static void main(String[] args) {
        String topic = "First_topic";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        // Assign and Seek is used to replay  or fetch a specific message....

        TopicPartition partitionToRead = new TopicPartition(topic,0);
        consumer.assign(Arrays.asList(partitionToRead));

        long offsetToRead = 15L;
        consumer.seek(partitionToRead, offsetToRead);

        int noofMsgToRead = 5;
        while(noofMsgToRead>0)
        {
          ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100));
          for(ConsumerRecord<String, String> record: records)
          {    noofMsgToRead--;
                logger.info("Key :"+record.key()+" --- Value :"+record.value() +"----Partiton:"+record.partition()+" ---Offset:"
                +record.offset());
                if(noofMsgToRead==0)
                        break;
          }
        }

    }

}
