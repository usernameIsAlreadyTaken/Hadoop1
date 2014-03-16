package org.dataguru.kpi.pv;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class PV {

	public static void main(String[] args) throws Exception {
		String input = "hdfs://localhost:9000/zhoujun/dataguru/kpi";
		String output = "hdfs://localhost:9000/zhoujun/dataguru/kpi/pvoutput";

		JobConf conf = new JobConf(PV.class);
		conf.setJobName("KPIPV");
		//conf.addResource("classpath:/hadoop/core-site.xml");
		//conf.addResource("classpath:/hadoop/hdfs-site.xml");
		//conf.addResource("classpath:/hadoop/mapred-site.xml");

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(KPIPVMapper.class);
		conf.setCombinerClass(KPIPVReducer.class);
		conf.setReducerClass(KPIPVReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		JobClient.runJob(conf);
		System.exit(0);
	}

}
