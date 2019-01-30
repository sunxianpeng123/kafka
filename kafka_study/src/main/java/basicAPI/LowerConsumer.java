package basicAPI;

import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
/**
 * \* Created with IntelliJ IDEA.
 * \* User: sunxianpeng
 * \* Date: 2019/1/30
 * \* Time: 21:24
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class LowerConsumer {
    /*
     *根据指定的topic partition  offset 获取数据
     */
    public static void main(String[] args){
//        参数
        //kafka集群
     ArrayList<String> brokers=   new ArrayList<>();
        brokers.add("master");
        brokers.add("slave1");
        brokers.add("slave2");
        //端口号
        int port =9092;
//        主题
        String topic ="first";
//        分区
        int partition=0;
//        offset
        Long offset =2L;



    }

//    寻找主副本
    private BrokerEndPoint findLeader(List<String> brokers, int port, String topic, int partition, Long offset){

        for(String broker:brokers){
            //         创建获取分区leader的消费者对象
            SimpleConsumer getLeader=new SimpleConsumer(broker,port,1000,
                    1024*4,"getLeader");
//            创建一个主题元数据信息请求
            TopicMetadataRequest topicMetadataRequest=new TopicMetadataRequest(Collections.singletonList(topic));
//            获取元数据返回值
            TopicMetadataResponse metadataResponse=getLeader.send(topicMetadataRequest);
//            解析元数据返回值
            List<TopicMetadata> topicMetadataList=metadataResponse.topicsMetadata();
//            遍历主题元数据
            for(TopicMetadata topicMetadata:topicMetadataList){
//                获取多个分区的元数据信息
                List<PartitionMetadata> partitionMetadataList = topicMetadata.partitionsMetadata();
//                遍历分区元数据
                for(PartitionMetadata partitionMetadata:partitionMetadataList){
                    if (partition==partitionMetadata.partitionId()){
//                        partitionMetadata.replicas();//
                        return  partitionMetadata.leader();
                    }
                }

            }

        }
        return null;
    }
//    获取数据
    private  void getData(){

    }

}