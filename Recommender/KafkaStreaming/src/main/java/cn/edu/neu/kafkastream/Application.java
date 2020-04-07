package cn.edu.neu.kafkastream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class Application {
    public static void main(String[] args) {
        String brokers = "localhost:9092";

        // 定义输入和输出的topic
        String from = "log";
        String to = "recommender";

        // 定义kafka streaming的配置
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        StreamsConfig config = new StreamsConfig(props);
        // 拓扑建构器
        Topology topology = new Topology();
        // 定义流处理的拓扑结构
        topology.addSource("SOURCE", from)
                .addProcessor("PROCESS", () -> new LogProcessor(), "SOURCE") //lamda
                .addSink("SINK", to, "PROCESS");

        //创建kafka stream
        KafkaStreams kafkaStreams = new KafkaStreams(topology,props);
        kafkaStreams.start();
        System.out.println("kafka stream started");
    }
}
