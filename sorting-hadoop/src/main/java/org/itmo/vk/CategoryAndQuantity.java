package org.itmo.vk;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CategoryAndQuantity implements Writable {
    private Text category = new Text();
    private DoubleWritable totalQuantity = new DoubleWritable();

    public CategoryAndQuantity() {}

    public CategoryAndQuantity(String totalSum, double totalQuantity) {
        this.category = new Text(totalSum);
        this.totalQuantity  = new DoubleWritable(totalQuantity);
    }

    public Text getCategory() {
        return category;
    }

    public void setCategory(Text category) {
        this.category = category;
    }

    public DoubleWritable getTotalQuantity() {
        return totalQuantity;
    }

    public void setTotalQuantity(DoubleWritable totalQuantity) {
        this.totalQuantity = totalQuantity;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.category.write(out);
        this.totalQuantity.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.category.readFields(in);
        this.totalQuantity.readFields(in);
    }

    @Override
    public String toString() {
        return category + "\t" + totalQuantity;
    }
}