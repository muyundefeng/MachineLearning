package com.muyundefeng.cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.VectorWritable;

/**
 * 将文本转化为向量
 * Created by lisheng on 17-4-17.
 */
public class Text2VectorWritable extends AbstractJob {


    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new Text2VectorWritable(), args);
    }

    public int run(String[] strings) throws Exception {
        addInputOption();//添加输入路径,对应命令终端的"-i"
        addOutputOption();//添加输出路径,对应命令终端的"-o"
        if (parseArguments(strings) == null)
            return -1;
        Path input = getInputPath();
        Path output = getOutputPath();
        Configuration configuration = getConf();
        Job job = new Job(configuration, "text2vector with input" + input.getName());
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setMapperClass(Text2VectorWritableMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(VectorWritable.class);
        job.setReducerClass(Text2VectorReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(VectorWritable.class);
        job.setJarByClass(Text2VectorWritable.class);
        FileInputFormat.addInputPath(job, input);
        SequenceFileOutputFormat.setOutputPath(job, output);
        if (!job.waitForCompletion(true)) {
            System.out.println("error");
        }
        return 0;
    }
}
