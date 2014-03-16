package org.dataguru.kpi.pv;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.dataguru.kpi.KPI;

public class KPIPVMapper extends MapReduceBase implements
		Mapper<Object, Text, Text, IntWritable> {

	private IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	public void map(Object key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		KPI kpi = KPI.filterPVs(value.toString());
		if (kpi.isValid()) {
			word.set(kpi.getRequest());
			output.collect(word, one);
		}
	}

}
