package org.itmo.vk;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortingReducer extends Reducer<DoubleWritable, CategoryAndQuantity, Text, CategoryData> {
    @Override
    protected void reduce(DoubleWritable key, Iterable<CategoryAndQuantity> values, Context context) throws IOException, InterruptedException {
        CategoryAndQuantity data = values.iterator().next();
        Text category = data.getCategory();
        DoubleWritable quantity = data.getTotalQuantity();
        CategoryData result = new CategoryData(key.get(), quantity.get());

        context.write(category, result);
    }
}