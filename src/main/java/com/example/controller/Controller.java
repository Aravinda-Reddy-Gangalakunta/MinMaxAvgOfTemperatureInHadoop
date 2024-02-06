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

	public static void main(String[] args) throws Exception {
//        if (args.length != 4) {
//            System.err.println("Usage: MonthlyAvgTemp <year month> <input path> <output path>");
//            System.exit(-1);
//        }
		System.out.println("args length: "+args.length);

        Configuration conf = new Configuration();
        conf.set("month", args[0]);

        Job job = Job.getInstance(conf, "Monthly Average Temperature");
        job.setJarByClass(Controller.class);
        job.setMapperClass(TemperatureMapper.class);
        job.setReducerClass(TemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
