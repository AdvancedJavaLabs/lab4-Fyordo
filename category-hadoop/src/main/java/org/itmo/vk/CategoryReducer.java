package org.itmo.vk;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CategoryReducer extends Reducer<Text, CategoryData, Text, CategoryData> {
    @Override
    protected void reduce(Text key, Iterable<CategoryData> values, Context context) throws IOException, InterruptedException {
        double totalSum = 0.0;
        double totalQuantity = 0D;

        for (CategoryData value : values) {
            DoubleWritable sum = value.getTotalSum();
            DoubleWritable quantity = value.getTotalQuantity();

            totalSum += sum.get();
            totalQuantity += quantity.get();
        }

        CategoryData result = new CategoryData(totalSum, totalQuantity);

        context.write(key, result);
    }
}