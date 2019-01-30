package demo;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: sunxianpeng
 * \* Date: 2019/1/30
 * \* Time: 18:50
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class ConsumerPartitioner implements Partitioner {
    //重新分区
    private  Map configMap =null;
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
            configMap=configs;
    }
}