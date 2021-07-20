import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RankReducer extends Reducer<FloatPair, Text, Text, Text> {
	@Override
	protected void reduce(FloatPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text v : values) {
			String[] split = v.toString().split("\t");
			String first = String.valueOf(key.getFirst());
			float second = key.getSecond();
			if (split.length == 2) {
				context.write(new Text(first), new Text(split[0]+"\t"+second+"\t"+split[1]));
			}
			else {
				context.write(new Text(first), new Text(split[0]+"\t"+second));
			}
		}
	}
}
