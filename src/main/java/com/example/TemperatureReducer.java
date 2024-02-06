package com.example;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

// This class extends the Hadoop Reducer class to perform the reduce operation
public class TemperatureReducer extends Reducer<Text, Text, Text, Text>{

    /**
     * Reduces the values for a given key.
     * Calculates the sum of maximum and minimum temperatures and the count of each.
     * Calculates the average maximum and minimum temperatures.
     * Adjusts the output format to: Day, AvgMax, AvgMin.
     *
     * @param key      the input key.
     * @param values   the input values.
     * @param context  the context object for writing the output.
     * @throws IOException          if an I/O error occurs.
     * @throws InterruptedException if the thread is interrupted.
     */
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Initialize the sum and count for maximum and minimum temperatures
        double sumMax = 0;
        double sumMin = 0;
        int maxCount = 0;
        int minCount = 0;
        
        // Iterate over the values for the given key
        for (Text val : values) {
            // Split the value into parts based on the underscore delimiter
            String[] parts = val.toString().split("_");
            
            // If the first part is "TMAX", add the second part to the sum of maximum temperatures and increment the count
            if(parts[0].equals("TMAX")) {
                sumMax += Double.parseDouble(parts[1]);
                maxCount++;
            } 
            // If the first part is "TMIN", add the second part to the sum of minimum temperatures and increment the count
            else if(parts[0].equals("TMIN")) {
                sumMin += Double.parseDouble(parts[1]);
                minCount++;
            }
        }
        
        // Calculate the average maximum and minimum temperatures
        double avgMax = maxCount > 0 ? sumMax / maxCount : 0;
        double avgMin = minCount > 0 ? sumMin / minCount : 0;
        
        // Write the key and the average maximum and minimum temperatures to the context
        // The output format is: Day, AvgMax, AvgMin
        context.write(key, new Text(avgMax + ", " + avgMin));
    }

}