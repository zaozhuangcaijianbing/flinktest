package kafkastream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author wangning
 * 从kafka获取
 */
public class KafkaWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.199.188:9092");
        properties.setProperty("group.id", "flink_word_count");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("truck_collector_alarm_info", new SimpleStringSchema(), properties));

        stream.flatMap(new Splitter())
                .keyBy(0)
                .timeWindow(Time.seconds(10))
                .sum(1);

        stream.print();
//        stream.writeAsText("e://aa.txt");

        env.execute("kafka wordcount");
    }


    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
