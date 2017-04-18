package com.muyundefeng.cluster;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

/**
 * Created by lisheng on 17-4-17.
 */
public class Text2VectorReducer extends Reducer<LongWritable,VectorWritable,LongWritable,VectorWritable> {
    @Override
    protected void reduce(LongWritable key, Iterable<VectorWritable> values, Context context) throws IOException, InterruptedException {
        for(VectorWritable value:values)
            context.write(key,value);
    }
}
