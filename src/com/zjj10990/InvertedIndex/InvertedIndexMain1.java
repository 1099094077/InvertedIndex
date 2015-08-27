package com.zjj10990.InvertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndexMain1 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		args = new String[]{
			"hdfs://hadoop:9000/input/InvertedIndex1",
			"hdfs://hadoop:9000/output/InvertedIndex1"
		};
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = new Job(conf);
		job.setJarByClass(InvertedIndexMain1.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(InvertedIndexMapper1.class);
		job.setCombinerClass(InvertedIndexCombiner.class);
		job.setReducerClass(InvertedIndexReducer1.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		Boolean isSuccess = job.waitForCompletion(true);
		System.exit(isSuccess?1:0);
	}
}
