package mytest1031;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

import static javafx.scene.input.KeyCode.R;

public class KafkaProducerTest {
    public static void main(String[] args) throws Exception {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "192.168.199.188:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(kafkaProps);


        ArrayList<String> appTypeList = Lists.newArrayList("android", "ios","未知");
        ArrayList<String> provinceList = Lists.newArrayList("山东", "江苏","上海","北京","安徽","浙江","香港");
        ArrayList<String> sexList = Lists.newArrayList("男", "女");

        Random random  = new Random();

        for (int i = 0; i < 10000; i++) {

            LogPojo logPojo = new LogPojo();
            logPojo.setAppType(appTypeList.get(random.nextInt(appTypeList.size())));
            logPojo.setProvince(provinceList.get(random.nextInt(provinceList.size())));
            logPojo.setSex(sexList.get(random.nextInt(sexList.size())));
            logPojo.setTime(System.currentTimeMillis());
            logPojo.setPrice(random.nextDouble());

            ProducerRecord record = new ProducerRecord("wn_1031", JSONObject.toJSONString(logPojo));
            producer.send(record);
            Thread.sleep(100);

        }
    }
}
