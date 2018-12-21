package time;

import kafkastream.KafkaWordCount;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class TimeTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.199.188:9092");
        properties.setProperty("group.id", "time_test");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("truck_collector_alarm_info", new SimpleStringSchema(), properties));


        stream.flatMap(new KafkaWordCount.Splitter())
                .keyBy(1)
                .timeWindow(Time.seconds(10))
                .sum(1);

        stream.print();
        env.execute("TimeTest");
    }

}
