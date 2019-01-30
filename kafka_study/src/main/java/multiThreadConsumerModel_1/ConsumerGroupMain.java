package multiThreadConsumerModel_1;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: sunxianpeng
 * \* Date: 2019/1/27
 * \* Time: 21:09
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class ConsumerGroupMain {
    //模型一：多个Consumer且每一个Consumer有自己的线程，对应的架构图如下：
    // ConsumerGroupMain.java是入口类
    //优点
//    1.Consumer Group容易实现
//2.各个Partition的顺序实现更容易
    //缺点
//    1.Consumer的数量不能超过Partition的数量，否则多出的Consumer永远不会被使用到
//2.因没个Consumer都需要一个TCP链接，会造成大量的系统性能损耗
    public static void main(String[] args){
        String brokers = "192.168.199.131:9092,192.168.199.132:9092,192.168.199.134:9092";
        String groupId = "group01";
        String topic = "HelloWorld";
        int consumerNumber = 3;

        Thread producerThread = new Thread(new ProducerThread(brokers,topic));
        producerThread.start();

        ConsumerGroup consumerGroup = new ConsumerGroup(brokers,groupId,topic,consumerNumber);
        consumerGroup.start();
    }
}