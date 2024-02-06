package com.example.controller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.example.TemperatureMapper;
import com.example.TemperatureReducer;

public class Controller {

    /**
     * The main method is the entry point of the program.
     * It sets up the configuration, job, and input/output paths for calculating the monthly average temperature.
     * It also runs the job and waits for its completion.
     *
     * @param args The command line arguments. The first argument is the month, the second argument is the input path, and the third argument is the output path.
     * @throws Exception If there is an error in running the job.
     */
    public static void main(String[] args) throws Exception {

        // Create a new Hadoop Configuration object
        Configuration conf = new Configuration();

        // Set the "month" configuration parameter to the first command line argument
        conf.set("month", args[0]);

        // Create a new Job instance with the given configuration and job name
        Job job = Job.getInstance(conf, "Monthly Average Temperature");

        // Set the JAR file for the job to the JAR file of the Controller class
        job.setJarByClass(Controller.class);

        // Set the Mapper class for the job to the TemperatureMapper class
        job.setMapperClass(TemperatureMapper.class);

        // Set the Reducer class for the job to the TemperatureReducer class
        job.setReducerClass(TemperatureReducer.class);

        // Set the output key class for the job to the Text class
        job.setOutputKeyClass(Text.class);

        // Set the output value class for the job to the Text class
        job.setOutputValueClass(Text.class);

        // Add the input path for the job to the second command line argument
        FileInputFormat.addInputPath(job, new Path(args[1]));

        // Set the output path for the job to the third command line argument
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Run the job and wait for its completion, then exit with the appropriate status code
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}