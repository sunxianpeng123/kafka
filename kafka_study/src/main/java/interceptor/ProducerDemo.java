package interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: sunxianpeng
 * \* Date: 2019/1/26
 * \* Time: 16:10
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \+
 */
public class ProducerDemo {
    public static void main(String[] args){
        Properties properties = new Properties();
        //bootstrap.servers是Kafka集群的IP地址，如果Broker数量超过1个，则使用逗号分隔，如"192.168.1.110:9092,192.168.1.110:9092"
        properties.put("bootstrap.servers", "192.168.199.131:9092,192.168.199.132:9092,192.168.199.134:9092");
        //应答级别，0只发送不确认，1只接受partition的leader的确认，all接受partition的leader和follower的确认
//               在类ProducerConfig中查看所有的生产者配置
//        ProducerConfig
//        properties.put("acks", "all");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        //重试次数
        properties.put("retries", 0);
        //批量大小，一次发送缓存
        properties.put("batch.size", 16384);
        //提交延时
        properties.put("linger.ms", 1);
        //producer总的缓存大小
        properties.put("buffer.memory", 33554432);
        //自动以发送分区
        //序列化类型。 Kafka消息是以键值对的形式发送到Kafka集群的，其中Key是可选的，Value可以是任意类型。
        // 但是在Message被发送到Kafka集群之前，Producer需要把不同类型的消
        // 息序列化为二进制类型。本例是发送文本消息到Kafka集群，所以使用的是StringSerializer。
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        ArrayList<String> list =new ArrayList<>();
        list.add("interceptor.TimeIntercetor");
        list.add("interceptor.CountIntercetor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,list);
        Producer<String, String> producer = null;
        String topic ="second";
//        发送100个消息到HelloWorld这个Topic
        try {
//            创建生产者对象
            producer = new KafkaProducer<String, String>(properties);
            for (int i = 0; i < 10; i++) {
                String msg = "Message " + i;
//                不带回调函数的生产者
//                producer.send(new ProducerRecord<String, String>("HelloWorld", msg));
                //带有回调函数的生产者,发送完毕后调用函数，打印信息
                producer.send(new ProducerRecord<String, String>(topic,msg), (metadata, exception) -> {
                    if (exception==null){
                        System.out.println(metadata.partition()+"--"+metadata.offset());
                    }else{
                        System.out.println("发送失败");
                    }
                });
                System.out.println("Sent:" + msg);
            }
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            producer.close();
        }

    }

}