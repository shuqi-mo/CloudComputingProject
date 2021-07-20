import java.io.IOException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixMultiplicationMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private static ArrayList<String> list=new ArrayList<String>();
	private static String[] items;
	// 初始化items：保存所有物品的编号
	static{
		String filePath = "hdfs://master:9000/itemcf/small.csv";
			try {
				FileSystem fs = FileSystem.get(URI.create("hdfs://master:9000"), new Configuration());
				FSDataInputStream read = fs.open(new Path(filePath));
				InputStreamReader in = new InputStreamReader(read);
				BufferedReader br = new BufferedReader(in);
				String str;
				while ((str = br.readLine()) != null && str.length() != 0) {
					String[] split = str.split(",");
					if (!(list.contains(split[1]))) {
						list.add(split[1]);
					}
				}
				items = new String[list.size()];
				int n = 0;
				for (String i : list) {
					items[n++] = i;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
	}
	
	@Override
	protected void map(LongWritable inKey,Text inValue,Context context) throws IOException, InterruptedException{
		String line = inValue.toString();
		StringTokenizer st = new StringTokenizer(line, "\t");
		String flagStr = st.nextToken();
		// 读取的是共现矩阵
		if (flagStr.contains(":")) {
			String[] split = flagStr.split(":");
			String key = split[0];
			String value = "";
			// 标记为A矩阵
			while (st.hasMoreTokens()) {
				value += "A" + ":" + split[1] + ":" + st.nextToken();
			}
			context.write(new Text(key), new Text(value));
		}
		// 读取的是评分矩阵
		else {
			for(String i : items) {
				String value = "";
				// 标记为B矩阵
				value += "B" + ":" + line;
				context.write(new Text(i), new Text(value));
			}
		}
		
		
	}
}
