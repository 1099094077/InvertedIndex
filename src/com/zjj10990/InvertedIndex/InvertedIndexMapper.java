package com.zjj10990.InvertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String lineValue = value.toString();
		String[] strs = lineValue.split("\t");
		
		context.write(new Text(strs[1]), new Text(strs[0]));
	}
	
}
