package mytest1031;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 *
 * 假设我们有一个数据源，它监控系统中订单的情况，当有新订单时，
 * 它使用 Tuple2<String, Integer> 输出订单中商品的类型和交易额。
 * 然后，我们希望实时统计每个类别的交易额，以及实时统计全部类别的交易额。
 * 基于 Environment 对象，我们首先创建了一个 Source，从而得到初始的<商品类型，成交量>流。然后，为了统计每种类别的成交量，我们使用 KeyBy 按 Tuple 的第 1 个字段（即商品类型）对输入流进行分组，并对每一个 Key 对应的记录的第 2 个字段（即成交量）进行求合。在底层，Sum 算子内部会使用 State 来维护每个Key（即商品类型）对应的成交量之和。当有新记录到达时，Sum 算子内部会更新所维护的成交量之和，并输出一条<商品类型，更新后的成交量>记录。
 *
 *
 *
 * 如果只统计各个类型的成交量，则程序可以到此为止，我们可以直接在 Sum 后添加一个 Sink 算子对不断更新的各类型成交量进行输出。但是，我们还需要统计所有类型的总成交量。为了做到这一点，我们需要将所有记录输出到同一个计算节点的实例上。我们可以通过 KeyBy 并且对所有记录返回同一个 Key，将所有记录分到同一个组中，从而可以全部发送到同一个实例上。
 *
 *
 *
 * 然后，我们使用 Fold 方法来在算子中维护每种类型商品的成交量。注意虽然目前 Fold 方法已经被标记为 Deprecated，但是在 DataStream API 中暂时还没有能替代它的其它操作，所以我们仍然使用 Fold 方法。这一方法接收一个初始值，然后当后续流中每条记录到达的时候，算子会调用所传递的 FoldFunction 对初始值进行更新，并发送更新后的值。我们使用一个 HashMap 来对各个类别的当前成交量进行维护，当有一条新的<商品类别，成交量>到达时，我们就更新该 HashMap。这样在 Sink 中，我们收到的是最新的商品类别和成交量的 HashMap，我们可以依赖这个值来输出各个商品的成交量和总的成交量。
 */
public class GroupedProcessingTimeWindowSample {
    private static class DataSource extends RichParallelSourceFunction<Tuple2<String, Integer>> {
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceFunction.SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
            Random random = new Random();
            while (isRunning) {
                Thread.sleep((getRuntimeContext().getIndexOfThisSubtask() + 1) * 1000 * 5);
                String key = "类别" + (char) ('A' + random.nextInt(3));
                int value = random.nextInt(10) + 1;

                System.out.println(String.format("Emits\t(%s, %d)", key, value));
                ctx.collect(new Tuple2<>(key, value));
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<Tuple2<String, Integer>> ds = env.addSource(new DataSource());
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = ds.keyBy(0);

        keyedStream.sum(1).keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return "";
            }
        }).fold(new HashMap<String, Integer>(), new FoldFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> fold(HashMap<String, Integer> accumulator, Tuple2<String, Integer> value) throws Exception {
                accumulator.put(value.f0, value.f1);
                return accumulator;
            }
        }).addSink(new SinkFunction<HashMap<String, Integer>>() {
            @Override
            public void invoke(HashMap<String, Integer> value, SinkFunction.Context context) throws Exception {
                // 每个类型的商品成交量
                System.out.println(value);
                // 商品成交总量
                System.out.println(value.values().stream().mapToInt(v -> v).sum());
            }
        });

        env.execute();
    }
}
