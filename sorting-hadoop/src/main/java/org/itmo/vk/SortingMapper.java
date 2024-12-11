package org.itmo.vk;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SortingMapper extends Mapper<Object, Text, DoubleWritable, CategoryAndQuantity> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        System.out.println("ZALUPA " + value);
        if (fields.length == 3) {
            String categoryName = fields[0];
            double sum = Double.parseDouble(fields[1]);
            double quantity = Double.parseDouble(fields[2]);

            context.write(new DoubleWritable(sum), new CategoryAndQuantity(categoryName, quantity));
        }
    }
}