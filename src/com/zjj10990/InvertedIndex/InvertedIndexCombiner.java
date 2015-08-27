package com.zjj10990.InvertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexCombiner extends
		Reducer<Text, Text, Text, Text> {
	private Text info = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException,
			InterruptedException {
		int sum = 0;
		for(Text value : values){
			sum += Integer.parseInt(value.toString());
		}
		String keyInfo = key.toString();
		String[] temps = keyInfo.split(":");
		key.set(temps[0]);
		info.set(temps[1]+":"+sum);
		context.write(key, info);
	}
	
}
