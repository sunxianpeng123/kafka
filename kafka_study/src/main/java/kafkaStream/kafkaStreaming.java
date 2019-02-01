package kafkaStream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: sunxianpeng
 * \* Date: 2019/1/31
 * \* Time: 14:47
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class kafkaStreaming  {
    public static void main(String[] args) {
//        创建拓扑对象
        TopologyBuilder builder= new TopologyBuilder();
//        创建配置文件
        Properties properties = new Properties();
        properties.put("application.id","kafka_streaming_test");
        properties.put("bootstrap.servers","192.168.199.131:9092,192.168.199.132:9092,192.168.199.134:9092");
//        构建拓扑结构
        builder.addSource("source","first")
                .addProcessor("processor", () -> new LogProcessor(), "source")
                .addSink("sink","second","processor");
        KafkaStreams kafkaStreams = new KafkaStreams(builder,properties);
        kafkaStreams.start();
    }




}