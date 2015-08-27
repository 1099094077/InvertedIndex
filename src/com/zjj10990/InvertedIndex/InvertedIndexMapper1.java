package com.zjj10990.InvertedIndex;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper1 extends Mapper<LongWritable, Text, Text, Text>{
	private Text keyInfo = new Text();
	private Text valueInfo = new Text();
	private FileSplit split;
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		split = (FileSplit) context.getInputSplit();
		String lineValue = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(lineValue);
		while (tokenizer.hasMoreTokens()) {
			keyInfo.set(tokenizer.nextToken()+":"+split.getPath().getName());
			valueInfo.set("1");
			context.write(keyInfo,valueInfo);
		}
	}
	
}
