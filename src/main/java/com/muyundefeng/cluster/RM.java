package com.muyundefeng.cluster;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.iterator.ClusterWritable;

import java.io.IOException;


/**
 * Created by lisheng on 17-4-17.
 */
public class RM extends Mapper<Text,ClusterWritable,Text, Text> {
    @Override
    protected void map(Text key, ClusterWritable value, Context context) throws IOException, InterruptedException {
        String str = value.getValue().getCenter().asFormatString();
        System.out.println("center****************:"+str);
        context.write(key,new Text(str));
    }
}
