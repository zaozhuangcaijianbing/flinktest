package watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

//3,13,23会打印一次结果
public class WikipediaAnalysisWaterMark {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());
        DataStream<WikipediaEditEvent> waterMarkStream =
                edits.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<WikipediaEditEvent>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(WikipediaEditEvent element) {
                        return element.getTimestamp();
                    }
                });


        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = waterMarkStream.map(new MapFunction<WikipediaEditEvent, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(WikipediaEditEvent value) throws Exception {
                return new Tuple2<>(value.getChannel(), 1);
            }
        }).keyBy(0)
                .timeWindow(Time.seconds(10))
                .sum(1);

        sum.print();

        see.execute();
    }
}
