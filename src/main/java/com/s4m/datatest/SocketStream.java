package com.s4m.datatest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.s4m.datatest.entity.Record;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SocketStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Record> stream = env
                .socketTextStream("localhost", 9999)
                .map(x -> getRecord(x));


        stream = stream
                .keyBy(value -> value.getId())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
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

        env.execute("Window WordCount");
    }

    private static Record getRecord(String value) {

        String[] recordValue = value.split(",");

        Record inputRecord = new Record();
        inputRecord.setId(new Long(recordValue[0]));
        inputRecord.setName(recordValue[1]);
        inputRecord.setTimestamp(new Long(recordValue[2]));

        return inputRecord;
    }

    public static String getObjectAsString(Record object) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(object);
        } catch (Exception e) {
            return null;
        }
    }

    /* Test Data
        1,USER1,100
        1,USER1,101
        1,USER1,102
        2,USER2,100
        2,USER2,101
     */

}
