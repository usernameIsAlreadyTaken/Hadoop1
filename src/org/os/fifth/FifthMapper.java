package org.os.fifth;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FifthMapper extends Mapper<Object, Text, Text, IntWritable> {

	private Text textKey = new Text("KEY");
	private IntWritable intValue = new IntWritable(0);
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		intValue.set(Integer.parseInt(value.toString()));
		context.write(textKey, intValue);
	}

}
