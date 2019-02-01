package interceptor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Properties;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: sunxianpeng
 * \* Date: 2019/1/26
 * \* Time: 16:17
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class ConsumerDemo {

    public static void main(String[] args) {
        Properties properties = new Properties();
        //和Producer一样，是指向Kafka集群的IP地址，以逗号分隔。
        properties.put("bootstrap.servers", "192.168.199.131:9092,192.168.199.132:9092,192.168.199.134:9092");
//        ConsumerConfig

        //Consumer分组ID
        properties.put("group.id", "group-11");
//      消费数据后，是否自动提交offset
        properties.put("enable.auto.commit", "true");
//       延时多久自动提交offset
        properties.put("auto.commit.interval.ms", "1000");//可能会产生重复消费问题
//
        properties.put("auto.offset.reset", "earliest");
//        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");


        properties.put("session.timeout.ms", "30000");
//      序列化。Consumer把来自Kafka集群的二进制消息反序列化为指定的类型。因本例中的Producer使用的是String类型，所以调用StringDeserializer来反序列化
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        String topic ="second";
//      订阅某个topic
        kafkaConsumer.subscribe(Arrays.asList(topic));
//        kafkaConsumer.subscribe(Collections.singleton("first"));//只消费一个
//        kafkaConsumer.assign(Collections.singleton(new TopicPartition("first",0)));
        //定位读取 定位某个topic的某个分区的offset
//        kafkaConsumer.seek(new TopicPartition(topic,0),2);
        //Consumer订阅了Topic为test的消息，Consumer调用poll方法来轮循Kafka集群的消息，其中的参数100是超时时间（Consumer等待直到Kafka集群中没有消息为止）：
        while (true) {
            ConsumerRecords<String, String> records  = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, value = %s topic= %s partition= %s",
                        record.offset(), record.value(), record.topic(), record.partition());
                System.out.println();
            }
        }
    }

}