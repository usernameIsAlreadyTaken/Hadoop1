package org.os.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class MyRecordReader extends RecordReader<LongWritable, Text> {

	private long start;
	private long pos;
	private long end;
	private FSDataInputStream fin = null;
	private LongWritable key = null;
	private Text value = null;
	private LineReader reader = null;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit) split;
		start = fileSplit.getStart();
		end = start + fileSplit.getLength();
		Configuration conf = context.getConfiguration();

		Path path = fileSplit.getPath();
		FileSystem fs = path.getFileSystem(conf);
		fin = fs.open(path);

		fin.seek(start);
		reader = new LineReader(fin);
		pos = 1;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (key == null) {
			key = new LongWritable();
		}
		key.set(pos);
		if (value == null) {
			value = new Text();
		}
		if (reader.readLine(value) == 0) {
			return false;
		}
		pos++;
		return true;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (getFilePosition() - start)
					/ (float) (end - start));
		}
	}

	@Override
	public void close() throws IOException {
		fin.close();
	}

	private long getFilePosition() {
		return pos;
	}
}
