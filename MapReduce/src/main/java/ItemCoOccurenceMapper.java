import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

public class ItemCoOccurenceMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	Text k = new Text();
	IntWritable one = new IntWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] strArr =  Pattern.compile("[\t,]").split(value.toString());
		for (int i = 1; i < strArr.length; i++) {
			String itemId1 = strArr[i].split(":")[0];
			for (int j = 1; j < strArr.length; j++) {
				String itemId2 = strArr[j].split(":")[0];
				k.set(itemId1 + ":" + itemId2);
				context.write(k, one);
			}
		}
	}
}
