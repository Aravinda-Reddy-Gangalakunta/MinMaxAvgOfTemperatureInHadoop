package com.example;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TemperatureMapper extends Mapper<LongWritable, Text, Text, Text>{

	private Text outKey = new Text();
    private Text outValue = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        // Assuming the format: StationID,Date,Type,Value
        String date = parts[1];
        String type = parts[2];
        String tempValue = parts[3];
        
        // Filter by YYYYMM provided as a parameter
        String targetMonth = context.getConfiguration().get("month");
        System.out.println("targetMonth: "+targetMonth+" date: "+date+" type: "+type+" tempValue: "+tempValue);
        if(date.startsWith(targetMonth)) {
            if(type.equals("TMAX") || type.equals("TMIN")) {
                outKey.set(date);
                outValue.set(type + "_" + tempValue);
                context.write(outKey, outValue);
            }
        }
    }
}
