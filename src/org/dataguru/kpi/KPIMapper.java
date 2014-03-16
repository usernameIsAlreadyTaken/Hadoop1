package org.dataguru.kpi;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KPIMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	private static String ip = "ip";
    private static String pv = "pv";
    private static String ps = "pageSize";// 页面大小
    private static String error = "parseError";// 解析出错
    private static String errorPV = "parsePV";// 解析出错
    
	private Text ipText = new Text(ip);
	private Text pvText = new Text(pv);
	private Text psText = new Text(ps);
	private Text errorText = new Text(error);
	private Text errorPVText = new Text(errorPV);
	private LongWritable one = new LongWritable(1);

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
			context.write(errorText, one);
			return;
		}
		System.out.println(ll);

		context.write(ipText, one);
		context.write(psText, new LongWritable(ll.getPageSize()));
		if (ll.getStatus().equals("200")) {
			context.write(pvText, one);
		} else {
			context.write(errorPVText, one);
		}
	}

}
