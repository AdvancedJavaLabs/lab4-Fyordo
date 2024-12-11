package org.itmo.vk;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CategoryData implements Writable {
    private DoubleWritable totalSum = new DoubleWritable();
    private DoubleWritable totalQuantity = new DoubleWritable();

    public CategoryData() {}

    public CategoryData(double totalSum, double totalQuantity) {
        this.totalSum = new DoubleWritable(totalSum);
        this.totalQuantity  = new DoubleWritable(totalQuantity);
    }

    public DoubleWritable getTotalSum() {
        return totalSum;
    }

    public void setTotalSum(DoubleWritable totalSum) {
        this.totalSum = totalSum;
    }

    public DoubleWritable getTotalQuantity() {
        return totalQuantity;
    }

    public void setTotalQuantity(DoubleWritable totalQuantity) {
        this.totalQuantity = totalQuantity;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.totalSum.write(out);
        this.totalQuantity.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.totalSum.readFields(in);
        this.totalQuantity.readFields(in);
    }

    @Override
    public String toString() {
        return totalSum + "\t" + totalQuantity;
    }
}