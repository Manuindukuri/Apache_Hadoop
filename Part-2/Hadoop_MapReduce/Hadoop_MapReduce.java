/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Project/Maven2/JavaApp/src/main/java/${packagePath}/${mainClassName}.java to edit this template
 */

package com.mycompany.hadoop_mapreduce;

/**
 *
 * @author 91833
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Hadoop_MapReduce {
    public static class AccessCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private Text ipAddress = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(" ");

            if (tokens.length >= 1) {
                String ip = tokens[0];
                ipAddress.set(ip);
                context.write(ipAddress, ONE);
            }
        }
    }

    public static class AccessCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable totalCount = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            totalCount.set(count);
            context.write(key, totalCount);
        }
    }

    public static class AccessCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable partialCount = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            partialCount.set(count);
            context.write(key, partialCount);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Access Count");

        job.setJarByClass(Hadoop_MapReduce.class);
        job.setMapperClass(AccessCountMapper.class);
        job.setCombinerClass(AccessCountCombiner.class); // Set the combiner class
        job.setReducerClass(AccessCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("/logs/access.log")); // Specify the path to access.log in HDFS
        FileOutputFormat.setOutputPath(job, new Path("/output")); // Set the output directory path in HDFS

        boolean success = job.waitForCompletion(true);

        System.exit(success ? 0 : 1);
    }
}