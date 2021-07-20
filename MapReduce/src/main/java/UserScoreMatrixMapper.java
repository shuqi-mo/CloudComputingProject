import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;
import java.util.regex.Pattern;

public class UserScoreMatrixMapper extends Mapper<LongWritable,Text,Text,Text> {
	Text userId = new Text();
	Text itemprocess = new Text();
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] strArr =  Pattern.compile("[\t,]").split(value.toString());
		System.out.println(strArr[0]);
        userId.set(strArr[0]);
        itemprocess.set(strArr[1] + ":" + strArr[2]);
        context.write(userId, itemprocess);
	}
}
