/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Project/Maven2/JavaApp/src/main/java/${packagePath}/${mainClassName}.java to edit this template
 */

package com.mycompany.maxstockprice;

/**
 *
 * @author 91833
 */
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxStockPrice {

  public static class MaxStockPriceMapper extends Mapper<Object, Text, Text, FloatWritable> {

    private Text stockSymbol = new Text();
    private FloatWritable stockPrice = new FloatWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] tokens = value.toString().split(",");
      if (tokens.length >= 5) {
        String symbol = tokens[1];
        String priceHighStr = tokens[4];

        try {
          float priceHigh = Float.parseFloat(priceHighStr);
          stockSymbol.set(symbol);
          stockPrice.set(priceHigh);
          context.write(stockSymbol, stockPrice);
        } catch (NumberFormatException e) {
          System.err.println("Error parsing priceHigh value: " + priceHighStr);
          System.err.println("Input line: " + value.toString());
          e.printStackTrace();
        }
      } else {
        System.err.println("Invalid input line: " + value.toString());
      }
    }
  }

  public static class MaxStockPriceReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    private FloatWritable maxPrice = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      float max = Float.MIN_VALUE;

      // Find the maximum price for the stock symbol
      for (FloatWritable value : values) {
        max = Math.max(max, value.get());
      }

      maxPrice.set(max);
      context.write(key, maxPrice);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "MaxStockPrice");
    job.setJarByClass(MaxStockPrice.class);
    job.setMapperClass(MaxStockPriceMapper.class);
    job.setReducerClass(MaxStockPriceReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(job, new Path("/stocks/*.csv"));
    FileOutputFormat.setOutputPath(job, new Path("/output/nyse"));

    boolean success = job.waitForCompletion(true);

    System.exit(success ? 0 : 1);
  }
}
