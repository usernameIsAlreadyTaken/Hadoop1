package org.dataguru.kpi.domain;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DomainReducer extends Reducer<Text, Text, Text, IntWritable> {

	private Text textKey = new Text("");
	private IntWritable intValue = new IntWritable(0);

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Iterator<Text> iterator = values.iterator();
		Set<String> set = new HashSet<String>();
		while (iterator.hasNext()) {
			set.add(iterator.next().toString());
		}
		textKey.set(key + " : ");
		intValue.set(set.size());
		context.write(textKey, intValue);
	}

}
