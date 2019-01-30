package multiThreadConsumerModel_2;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: sunxianpeng
 * \* Date: 2019/1/27
 * \* Time: 21:18
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class ConsumerThread {
    //ConsumerThread.java是消费者使用线程池的方式初始化消费者线程
    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    // Threadpool of consumers
    private ExecutorService executor;


    public ConsumerThread(String brokers, String groupId, String topic){
        Properties properties = buildKafkaProperty(brokers,groupId);
        this.consumer = new KafkaConsumer<>(properties);
        this.topic = topic;
        this.consumer.subscribe(Arrays.asList(this.topic));
    }

    public void start(int threadNumber){
        executor = new ThreadPoolExecutor(threadNumber,threadNumber,0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
        while (true){
            ConsumerRecords<String,String> consumerRecords = consumer.poll(100);
            for (ConsumerRecord<String,String> item : consumerRecords){
                executor.submit(new ConsumerThreadHandler(item));
            }
        }
    }

    private static Properties buildKafkaProperty(String brokers, String groupId){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokers);
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "300000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return properties;
    }

}