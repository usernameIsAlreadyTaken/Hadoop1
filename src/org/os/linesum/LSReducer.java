package org.os.linesum;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LSReducer extends Reducer<LongWritable, Text, Text, IntWritable>{

	Text writeKey = new Text("");
	IntWritable writeValue = new IntWritable(0);
	
	@Override
	protected void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for(Text val: values){
			sum += Integer.parseInt(val.toString());
		}
		if(key.get() == 0){
			writeKey.set("sum of even lines: ");
		}else{
			writeKey.set("sum of odd lines: ");
		}
		writeValue.set(sum);
		context.write(writeKey, writeValue);
	}

}
