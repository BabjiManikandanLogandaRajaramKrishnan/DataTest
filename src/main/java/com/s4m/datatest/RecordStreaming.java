package com.s4m.datatest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.s4m.datatest.entity.Record;
import com.s4m.datatest.util.ConfigReader;
import com.s4m.datatest.util.RecordDeserializationSchema;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;

import java.util.Properties;

import static com.s4m.datatest.Constants.BOOTSTRAP_SERVER;

public class RecordStreaming {

    // Source https://nightlies.apache.org/flink/flink-docs-release-1.4/dev/connectors/kafka.html

    public static void main(String[] args) throws Exception {

        Properties properties = ConfigReader.readConfig("flinkStreaming");

        String TOPIC_IN = properties.getProperty("TOPIC");

        Properties props = new Properties();
        props.put("bootstrap.servers", properties.getProperty(BOOTSTRAP_SERVER));
        props.put("client.id", "flink-kafka-example");

        FlinkKafkaConsumer08<Record> kafkaConsumer = new FlinkKafkaConsumer08<>(TOPIC_IN, new RecordDeserializationSchema(), props);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // for latest
        kafkaConsumer.setStartFromLatest();

        DataStream<Record> stream = env.addSource(kafkaConsumer);

        //https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/overview/

        stream = stream
                .keyBy(value -> value.getId())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(60)))
                .reduce(new ReduceFunction<Record>() {
                    @Override
                    public Record reduce(Record record1, Record record2) throws Exception {
                        if (record1.getTimestamp() > record2.getTimestamp()) {
                            return record1;
                        } else {
                            return record2;
                        }
                    }
                });

        stream.map(x -> getObjectAsString(x)).print();

        env.enableCheckpointing(5000);
        env.execute("Flink Stream");

    }

    public static String getObjectAsString(Record object) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(object);
        } catch (Exception e) {
            return null;
        }
    }


}
