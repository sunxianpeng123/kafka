package interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: sunxianpeng
 * \* Date: 2019/1/31
 * \* Time: 14:15
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class TimeIntercetor implements ProducerInterceptor<String,String>{


    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        ProducerRecord producerRecord = new ProducerRecord(record.topic(), record.key(),
                System.currentTimeMillis() + "," + record.value());

        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}