package com.zjj10990.InvertedIndex;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndexMain {
	public static void main(String[] args) throws Exception {
		//获取配置、参数
		Configuration configuration = new Configuration();
		args = new String[] {
			"hdfs://hadoop:9000/input/invertedindex",
			"hdfs://hadoop:9000/output/invertedindex"
		};
		String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
		if(otherArgs.length!=2){
			System.out.println("Usage: Mean <in> <out>");
            System.exit(2);
		}
		//创建job
		Job job = new Job(configuration);
		job.setJarByClass(InvertedIndexMain.class);
		//设置mapper
		job.setMapperClass(InvertedIndexMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//设置reduccer
		job.setReducerClass(InvertedIndexReducer.class);
		//设置输入输出路径
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//提交
		Boolean isSuccess = job.waitForCompletion(true);
		System.exit(isSuccess?1:0);
	}
}
