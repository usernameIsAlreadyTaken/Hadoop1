package org.os.linesum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class LSPartitioner extends Partitioner<LongWritable, Text> {

	@Override
	public int getPartition(LongWritable key, Text value, int numPartitions) {
		if (key.get() % numPartitions == 0) {
			key.set(0);
			return 0;
		} else {
			key.set(1);
			return 1;
		}
	}

}
