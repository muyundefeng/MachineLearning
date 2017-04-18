package com.muyundefeng.cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

/**
 * Created by lisheng on 17-4-17.
 */
public class ReadClusterWritable extends AbstractJob {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(),new ReadClusterWritable(),args);
    }

    public int run(String[] strings) throws Exception {
        addInputOption();
        addOutputOption();
        if(parseArguments(strings) == null)
            return -1;
        Job job =new Job(getConf(),getInputPath().toString());
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(RM.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        job.setJarByClass(ReadClusterWritable.class);

        FileInputFormat.addInputPath(job,getInputPath());
        FileOutputFormat.setOutputPath(job,getOutputPath());
        if(!job.waitForCompletion(true))
            return -1;
        return 0;
    }
}
