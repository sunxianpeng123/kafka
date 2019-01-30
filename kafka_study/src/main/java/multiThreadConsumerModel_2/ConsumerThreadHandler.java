package multiThreadConsumerModel_2;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: sunxianpeng
 * \* Date: 2019/1/27
 * \* Time: 21:15
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class ConsumerThreadHandler implements Runnable{
    //ConsumerThreadHandler.java用于处理发送到消费者的消息

    private ConsumerRecord consumerRecord;

    public ConsumerThreadHandler(ConsumerRecord consumerRecord){
        this.consumerRecord = consumerRecord;
    }

    @Override
    public void run() {
        System.out.println("Consumer Message:"+consumerRecord.value()+",Partition:"+consumerRecord.partition()+"Offset:"+consumerRecord.offset());
    }
}