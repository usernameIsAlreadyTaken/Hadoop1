package org.os.fifth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FifthReducer extends Reducer<Text, IntWritable, Text, Text> {

	private Text textKey = new Text("the fifth max number is: ");
	//private IntWritable fifthNum = new IntWritable(0);
	private Text textValue = new Text();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		List<IntWritable> list = new ArrayList<IntWritable>();
		while(values.iterator().hasNext()){
			list.add(values.iterator().next());
		}
		//Collections.sort(list);
		//Collections.reverse(list);
		//fifthNum.set(list.get(4).get());
		textValue.set(values.iterator().toString());
		context.write(textKey, textValue);
	}

}
