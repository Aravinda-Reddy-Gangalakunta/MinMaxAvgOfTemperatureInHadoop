package com.example;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * The TemperatureMapper class is responsible for mapping input data to key-value pairs.
 * It extends the Mapper class and overrides the map() method.
 */
public class TemperatureMapper extends Mapper<LongWritable, Text, Text, Text>{

    private Text outKey = new Text();
    private Text outValue = new Text();

    /**
     * Maps input key-value pairs to output key-value pairs.
     * It filters the input data based on the provided month and temperature type (TMAX or TMIN).
     * The output key is the date and the output value is the temperature type and value.
     *
     * @param key     The input key.
     * @param value   The input value.
     * @param context The context object for writing the output key-value pairs.
     * @throws IOException          If an I/O error occurs.
     * @throws InterruptedException If the thread is interrupted.
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        // Assuming the format: StationID,Date,Type,Value
        String date = parts[1];
        String type = parts[2];
        String tempValue = parts[3];

        // Filter by YYYYMM provided as a parameter
        String targetMonth = context.getConfiguration().get("month");
        System.out.println("targetMonth: " + targetMonth + " date: " + date + " type: " + type + " tempValue: " + tempValue);
        if (date.startsWith(targetMonth)) {
            if (type.equals("TMAX") || type.equals("TMIN")) {
                outKey.set(date);
                outValue.set(type + "_" + tempValue);
                context.write(outKey, outValue);
            }
        }
    }
}
