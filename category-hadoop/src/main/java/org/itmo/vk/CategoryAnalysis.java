package org.itmo.vk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CategoryAnalysis {
    public static void main(String[] args) throws Exception {
        String inputDir = "/Users/lagutinfedor/Documents/hadoop_data/input";
        String outputDir = "/Users/lagutinfedor/Documents/hadoop_data/output_unordered";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Category Analysis");

        job.setJarByClass(CategoryAnalysis.class);

        job.setMapperClass(CategoryMapper.class);
        job.setReducerClass(CategoryReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CategoryData.class);

        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        if (job.waitForCompletion(true)) {
            System.exit(0);
        }
    }
}