package org.os.linesum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LineSum {

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Linesum <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "line sum");
		job.setJarByClass(LineSum.class);
		job.setInputFormatClass(LSFileInputFormat.class);
		job.setMapperClass(LSMapper.class);
		// job.setCombinerClass(LSReducer.class);
		job.setPartitionerClass(LSPartitioner.class);
		job.setReducerClass(LSReducer.class);
		job.setNumReduceTasks(2);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
