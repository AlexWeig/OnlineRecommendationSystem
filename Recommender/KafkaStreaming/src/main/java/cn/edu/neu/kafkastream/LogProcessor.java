package cn.edu.neu.kafkastream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcessor implements Processor<byte[], byte[]> {
    private ProcessorContext context;
    @Override
    public void init(ProcessorContext processorContext) {
        this.context = context;
    }

    @Override
    public void process(byte[] dummy, byte[] line) {
        //核心处理流程
        String input = new String(line);
        //提取数据，以固定的前缀过滤日志信息，提取后面的类容
        if(input.contains("PRODUCT_RATING_PREFIX:")){
            System.out.println("product rating coming!!!!" + input);
            input = input.split("PRODUCT_RATING_PREFIX:")[1].trim(); //以前缀为切分内容，取后面的部分[1]，strim()去除前后空格
            context.forward("logProcessor".getBytes(), input.getBytes());
        }
    }

    @Override
    public void close() {

    }

    public static void main(String[] args) {


    }
}
