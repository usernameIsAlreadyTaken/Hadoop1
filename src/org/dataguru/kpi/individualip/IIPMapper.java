package org.dataguru.kpi.individualip;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.dataguru.kpi.LogLine;

public class IIPMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text ipText = new Text("");
	
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
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
		
		ipText.set(ll.getIp());
		context.write(new Text("ip"), ipText);
	}

}
