package org.dataguru.kpi.domain;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.dataguru.kpi.LogLine;

public class DomainMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text domainText = new Text("");
	private Text ipText = new Text("");

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString().trim();
		LogLine ll = null;
		try {
			ll = LogLine.parse(line);
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (ll == null) {
			System.err.println("one null line");
			return;
		}
		System.out.println(ll);

		domainText.set(ll.getHttpReferer());
		ipText.set(ll.getIp());
		context.write(domainText, ipText);
	}

}
