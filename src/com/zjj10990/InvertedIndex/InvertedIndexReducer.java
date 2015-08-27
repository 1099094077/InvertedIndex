package com.zjj10990.InvertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException,
			InterruptedException {
		String tempValue = "";
		for(Text value:values){
			tempValue += value.toString()+"|";
		}
		context.write(key, new Text(tempValue));
	}
}
