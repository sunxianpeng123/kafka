package kafkaStream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import scala.math.Ordering;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: sunxianpeng
 * \* Date: 2019/1/31
 * \* Time: 14:57
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class LogProcessor implements Processor<byte[],byte[]> {
    private ProcessorContext context;
    @Override
    public void init(ProcessorContext processorContext) {
            context=processorContext;
    }

    @Override
    public void process(byte[] bytes, byte[] bytes2) {
        //1
//        获取一行数据
        String line = new String(bytes2);
//        去除“>>>”
        line=line.replaceAll(">>>","");
        bytes2 = line.getBytes();
        System.out.println(line);
//        发送到目的topic
        context.forward(bytes,bytes2);
    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}