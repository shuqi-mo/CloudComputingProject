import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ItemCoOccurenceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	IntWritable resCount = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable v : values) {
			sum += v.get();
		}
		resCount.set(sum);
		context.write(key, resCount);
	}
}
