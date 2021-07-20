import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserScoreMatrixReducer extends Reducer<Text, Text, Text, Text> {
	Text v = new Text();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String itemprocess = "";
		for (Text item : values) {
			itemprocess += "\t" + item.toString();
		}
		v.set(itemprocess.replaceFirst("\t", ""));
		context.write(key, v);
	}
}
