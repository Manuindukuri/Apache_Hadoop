/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Project/Maven2/JavaApp/src/main/java/${packagePath}/${mainClassName}.java to edit this template
 */

package com.mycompany.nysedataanalysis;

/**
 *
 * @author 91833
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NYSEDataAnalysis {
    public static class NYSEDataWritable implements Writable {
        private String maxStockVolumeDate;
        private String minStockVolumeDate;
        private double maxStockPriceAdjClose;

        public NYSEDataWritable() {
            // Default constructor required for serialization
        }

        public NYSEDataWritable(String maxStockVolumeDate, String minStockVolumeDate, double maxStockPriceAdjClose) {
            this.maxStockVolumeDate = maxStockVolumeDate;
            this.minStockVolumeDate = minStockVolumeDate;
            this.maxStockPriceAdjClose = maxStockPriceAdjClose;
        }

        public String getMaxStockVolumeDate() {
            return maxStockVolumeDate;
        }

        public String getMinStockVolumeDate() {
            return minStockVolumeDate;
        }

        public double getMaxStockPriceAdjClose() {
            return maxStockPriceAdjClose;
        }

        public void setMaxStockVolumeDate(String maxStockVolumeDate) {
            this.maxStockVolumeDate = maxStockVolumeDate;
        }

        public void setMinStockVolumeDate(String minStockVolumeDate) {
            this.minStockVolumeDate = minStockVolumeDate;
        }

        public void setMaxStockPriceAdjClose(double maxStockPriceAdjClose) {
            this.maxStockPriceAdjClose = maxStockPriceAdjClose;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(maxStockVolumeDate);
            dataOutput.writeUTF(minStockVolumeDate);
            dataOutput.writeDouble(maxStockPriceAdjClose);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            maxStockVolumeDate = dataInput.readUTF();
            minStockVolumeDate = dataInput.readUTF();
            maxStockPriceAdjClose = dataInput.readDouble();
        }

        @Override
        public String toString() {
            return maxStockVolumeDate + "|" + minStockVolumeDate + "|" + maxStockPriceAdjClose;
        }
    }

    public static class NYSEMapper extends Mapper<LongWritable, Text, Text, NYSEDataWritable> {
        private Text outputKey = new Text();
        private NYSEDataWritable outputValue = new NYSEDataWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the CSV line into fields
            String[] fields = value.toString().split(",");

            // Validate the number of fields in the record
            if (fields.length != 9) {
                // Invalid record, skip processing
                return;
            }

            // Extract the relevant fields
            String exchange = fields[0];
            String stockSymbol = fields[1];
            String date = fields[2];

            // Validate the stock volume field
            double stockVolume;
            try {
                stockVolume = Double.parseDouble(fields[7]);
            } catch (NumberFormatException e) {
                // Invalid stock volume, skip processing
                return;
            }

            double stockPriceAdjClose;
            try {
                stockPriceAdjClose = Double.parseDouble(fields[8]);
            } catch (NumberFormatException e) {
                // Invalid stock price adjusted close, skip processing
                return;
            }

            // Set the output key as the stock symbol
            outputKey.set(stockSymbol);

            // Set the output value with the required fields
            outputValue.setMaxStockVolumeDate(date);
            outputValue.setMinStockVolumeDate(date);
            outputValue.setMaxStockPriceAdjClose(stockPriceAdjClose);

            // Emit the key-value pair
            context.write(outputKey, outputValue);
        }
    }

    public static class NYSECombiner extends Reducer<Text, NYSEDataWritable, Text, NYSEDataWritable> {
        private NYSEDataWritable outputValue = new NYSEDataWritable();

        @Override
        protected void reduce(Text key, Iterable<NYSEDataWritable> values, Context context) throws IOException, InterruptedException {
            String maxVolumeDate = null;
            String minVolumeDate = null;
            double maxPriceAdjClose = Double.MIN_VALUE;

            for (NYSEDataWritable value : values) {
                // Find the date of the max stock volume
                if (maxVolumeDate == null || value.getMaxStockVolumeDate().compareTo(maxVolumeDate) > 0) {
                    maxVolumeDate = value.getMaxStockVolumeDate();
                }

                // Find the date of the min stock volume
                if (minVolumeDate == null || value.getMinStockVolumeDate().compareTo(minVolumeDate) < 0) {
                    minVolumeDate = value.getMinStockVolumeDate();
                }

                // Find the max stock price adjusted close
                if (value.getMaxStockPriceAdjClose() > maxPriceAdjClose) {
                    maxPriceAdjClose = value.getMaxStockPriceAdjClose();
                }
            }

            // Set the output value with the results
            outputValue.setMaxStockVolumeDate(maxVolumeDate);
            outputValue.setMinStockVolumeDate(minVolumeDate);
            outputValue.setMaxStockPriceAdjClose(maxPriceAdjClose);

            // Emit the key-value pair
            context.write(key, outputValue);
        }
    }

    public static class NYSEReducer extends Reducer<Text, NYSEDataWritable, Text, Text> {
        private Text outputValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<NYSEDataWritable> values, Context context) throws IOException, InterruptedException {
            String maxVolumeDate = null;
            String minVolumeDate = null;
            double maxPriceAdjClose = Double.MIN_VALUE;

            for (NYSEDataWritable value : values) {
                // Find the date of the max stock volume
                if (maxVolumeDate == null || value.getMaxStockVolumeDate().compareTo(maxVolumeDate) > 0) {
                    maxVolumeDate = value.getMaxStockVolumeDate();
                }

                // Find the date of the min stock volume
                if (minVolumeDate == null || value.getMinStockVolumeDate().compareTo(minVolumeDate) < 0) {
                    minVolumeDate = value.getMinStockVolumeDate();
                }

                // Find the max stock price adjusted close
                if (value.getMaxStockPriceAdjClose() > maxPriceAdjClose) {
                    maxPriceAdjClose = value.getMaxStockPriceAdjClose();
                }
            }

            // Create the output value by concatenating the results with a delimiter
            String output = maxVolumeDate + "|" + minVolumeDate + "|" + maxPriceAdjClose;
            outputValue.set(output);

            // Emit the key-value pair
            context.write(key, outputValue);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "NYSE Data Analysis");
        job.setJarByClass(NYSEDataAnalysis.class);
        job.setMapperClass(NYSEMapper.class);
        job.setCombinerClass(NYSECombiner.class);
        job.setReducerClass(NYSEReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NYSEDataWritable.class);
        FileInputFormat.addInputPath(job, new Path("/stocks/*.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
