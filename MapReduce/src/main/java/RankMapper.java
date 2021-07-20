import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;
import java.util.StringTokenizer;

public class RankMapper extends Mapper<LongWritable,Text,FloatPair,Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		FloatPair k = new FloatPair();
		String v = "";
		StringTokenizer st = new StringTokenizer(value.toString());
		float first = Float.parseFloat(st.nextToken());
		v += st.nextToken() + "\t";
		float second = Float.parseFloat(st.nextToken());
		k.set(first, second);
		while (st.hasMoreTokens()) {
			v += st.nextToken();
		}
		context.write(k, new Text(v));
	}
}