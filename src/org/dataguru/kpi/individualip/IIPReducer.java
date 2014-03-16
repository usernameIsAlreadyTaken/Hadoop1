package org.dataguru.kpi.individualip;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IIPReducer extends Reducer<Text, Text, Text, IntWritable> {

	private Text textKey = new Text("独立ip个数: ");
	private IntWritable intValue = new IntWritable(0);
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Set<String> set = new HashSet<String>();
		Iterator<Text> iterator = values.iterator();
		while (iterator.hasNext()) {
			set.add(iterator.next().toString());
		}
		intValue.set(set.size());
		context.write(textKey, intValue);
	}
	
}
