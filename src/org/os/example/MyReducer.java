package org.os.example;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<LongWritable, Text, Text, IntWritable> {

	
	@Override
	protected void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for(Text val: values){
			sum += Integer.parseInt(val.toString());
		}
		Text writeKey = new Text();
		IntWritable writeValue = new IntWritable();
			writeKey.set("sum of odd lines: ");
		if(key.get() == 0){
			writeKey.set("sum of even lines: ");
		}else{
			
		}
		writeValue.set(sum);
		context.write(writeKey, writeValue);
	}

}
