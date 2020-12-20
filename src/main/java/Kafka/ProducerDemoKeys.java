package Kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    static Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //Create Producer Properties...
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        for(int i=0;i<10;i++)
        {
            String topic = "First_topic";
            String value = "hello world : "+Integer.toString(i);
            String key = "id_"+Integer.toString(i);

        // Create Producer Record
        ProducerRecord<String,String> record = new ProducerRecord<String, String>(topic,key,value);

        // Send  - asynchronous
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // Execute every time  record is successfully sent or exception is thrown
                if(e==null)
                {
                    logger.info("Received Meta Data : \n "+"Topic :"+recordMetadata.topic()
                            +"\n Partition :"+recordMetadata.partition()
                            +"\n offset :"+ recordMetadata.offset()
                            +"\n Timestamp :"+ recordMetadata.timestamp());
                }
                else
                {
                    logger.error(e.getMessage());
                }
            }
        }).get();}
        producer.flush();
        producer.close();

    }


}
