package multiThreadConsumerModel_2;

import multiThreadConsumerModel_1.ProducerThread;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: sunxianpeng
 * \* Date: 2019/1/27
 * \* Time: 21:20
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class ConsumerThreadMain {
//型二：一个Consumer且有多个Worker线程
    //优点
    //1.由于通过线程池实现了Consumer，横向扩展更方便
    //缺点
//    1.在每个Partition上实现顺序处理更困难。
//    例如：同一个Partition上有两个待处理的Message需要被线程池中的2个线程消费掉，那这两个线程必须实现同步
    public static void main(String[] args){
        String brokers = "192.168.199.131:9092,192.168.199.132:9092,192.168.199.134:9092";
        String groupId = "group01";
        String topic = "HelloWorld";
        int consumerNumber = 3;


        Thread producerThread = new Thread(new ProducerThread(brokers,topic));
        producerThread.start();

        ConsumerThread consumerThread = new ConsumerThread(brokers,groupId,topic);
        consumerThread.start(3);


    }
}