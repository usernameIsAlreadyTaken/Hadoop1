package org.dataguru.kpi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KPIReducer extends Reducer<Text, LongWritable, Text, Text> {

	private static String ip = "ip";
	private static String pv = "pv";
	private static String ps = "pageSize";// 页面大小
	private static String errorPV = "parsePV";// 解析出错

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		Iterator<LongWritable> iterator = values.iterator();
		long sum = 0;
		while (iterator.hasNext()) {
			sum += iterator.next().get();
		}
		String k = key.toString();
		String kAlias = "未知错误";
		String unit = "个";
		if (ip.equals(k)) {
			kAlias = "ip count:";
		} else if (pv.equals(k)) {
			kAlias = "pv count";
		} else if (ps.equals(k)) {
			kAlias = "传输页面的总字节数";
			long pMsize = sum / 1024 / 1024;
			sum = pMsize;
			unit = "M";
		} else if (errorPV.equals(k)) {
			kAlias = "error pv count";
		}
		context.write(new Text(kAlias), new Text(sum + unit));
	}
}
