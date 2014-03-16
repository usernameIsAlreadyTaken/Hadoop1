package org.hadoop.sort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<IntPaire, IntWritable, Text, Text> {

	@Override
	protected void reduce(IntPaire key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		
		
		
	}

}
