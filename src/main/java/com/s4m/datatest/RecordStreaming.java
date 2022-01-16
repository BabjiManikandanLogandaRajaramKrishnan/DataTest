package com.s4m.datatest;

import com.s4m.datatest.entity.Record;
import com.s4m.datatest.util.RecordDeserializationSchema;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;

import java.util.Properties;

public class RecordStreaming {

    // Source https://nightlies.apache.org/flink/flink-docs-release-1.4/dev/connectors/kafka.html

    public static void main(String[] args) throws Exception{

        String TOPIC_IN = "TOPIC-IN";
        String BOOTSTRAP_SERVER = "localhost:9092";

        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
        props.put("client.id", "flink-kafka-example");

        FlinkKafkaConsumer08<Record> kafkaConsumer = new FlinkKafkaConsumer08<>(TOPIC_IN, new RecordDeserializationSchema(), props);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // for latest
        kafkaConsumer.setStartFromLatest();

        DataStream<Record> stream = env.addSource(kafkaConsumer);

        //https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/overview/

        stream = stream.keyBy(value -> value).reduce(new ReduceFunction<Record>() {
            @Override
            public Record reduce(Record value1, Record value2) throws Exception {
                if(value1.getTimestamp() > value2.getTimestamp()){
                    return value1;
                } else{
                    return value2;
                }
            }
        });

        stream.print();

        env.enableCheckpointing(5000);
        env.execute("Flink Stream");

    }


}
