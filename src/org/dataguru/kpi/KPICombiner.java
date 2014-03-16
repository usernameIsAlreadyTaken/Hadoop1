package org.dataguru.kpi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KPICombiner extends
		Reducer<Text, LongWritable, Text, LongWritable> {

	private LongWritable sumWritable = new LongWritable();

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		Iterator<LongWritable> iterator = values.iterator();
		long sum = 0;
		while (iterator.hasNext()) {
			sum += iterator.next().get();
		}
		sumWritable.set(+sum);
		System.out.println(key.toString() + ":" + sumWritable.get());
		context.write(key, sumWritable);
	}
}
