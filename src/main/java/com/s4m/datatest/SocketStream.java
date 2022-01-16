package com.s4m.datatest;

import com.s4m.datatest.entity.Record;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SocketStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stream = env
                .socketTextStream("localhost", 9999);


        DataStream<String> stream1 = stream
                .keyBy(value -> value)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
                .reduce(new ReduceFunction<String>() {
            @Override
            public String reduce(String value1, String value2) throws Exception {

                Record record1 = getRecord(value1);
                Record record2 = getRecord(value2);

                if(record1.getTimestamp() > record2.getTimestamp()){
                    return record1.toString();
                } else{
                    return record2.toString();
                }
            }
        });

        stream1.print();

        env.execute("Window WordCount");
    }

    private static Record getRecord(String value){

        String[] recordValue = value.split(",");

        Record inputRecord = new Record();
        inputRecord.setId(new Long(recordValue[0]));
        inputRecord.setName(recordValue[1]);
        inputRecord.setTimestamp(new Long(recordValue[2]));

        return inputRecord;
    }

}
