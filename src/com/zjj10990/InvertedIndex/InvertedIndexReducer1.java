package com.zjj10990.InvertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer1 extends Reducer<Text, Text, Text, Text>{
	private Text result = new Text();
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException,
			InterruptedException {
		String fileList = new String();
		for(Text value:values){
			fileList += value.toString()+"\t";
		}
		result.set(fileList);
		context.write(key, result);
	}

	
}
