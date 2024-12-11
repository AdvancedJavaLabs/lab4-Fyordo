package org.itmo.vk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CategorySorting {
    public static void main(String[] args) throws Exception {
        String inputDir = "/Users/lagutinfedor/Documents/hadoop_data/input_unordered";
        String outputDir = "/Users/lagutinfedor/Documents/hadoop_data/output";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Category Sorting");

        job.setJarByClass(CategorySorting.class);

        job.setMapperClass(SortingMapper.class);
        job.setReducerClass(SortingReducer.class);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(CategoryAndQuantity.class);

        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        if (job.waitForCompletion(true)) {
            System.exit(0);
        }
    }
}