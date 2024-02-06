package com.example;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TemperatureReducer extends Reducer<Text, Text, Text, Text>{

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double sumMax = 0;
        double sumMin = 0;
        int maxCount = 0;
        int minCount = 0;
        
        for (Text val : values) {
            String[] parts = val.toString().split("_");
            if(parts[0].equals("TMAX")) {
                sumMax += Double.parseDouble(parts[1]);
                maxCount++;
            } else if(parts[0].equals("TMIN")) {
                sumMin += Double.parseDouble(parts[1]);
                minCount++;
            }
        }
        
        double avgMax = maxCount > 0 ? sumMax / maxCount : 0;
        double avgMin = minCount > 0 ? sumMin / minCount : 0;
        
        // Adjusting output format to: Day, AvgMax, AvgMin
        context.write(key, new Text(avgMax + ", " + avgMin));
    }

}
