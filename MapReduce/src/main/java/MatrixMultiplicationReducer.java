import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.Map.*;
import java.util.regex.Pattern;

public class MatrixMultiplicationReducer extends Reducer<Text, Text, Text, Text> {
	private Map<String,String> mapB = new LinkedHashMap<String,String>();
	private boolean flag = true;	// 标记用户是否已评分
	
	@Override
	protected void reduce(Text inKey, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		Map<String,String> mapA = new LinkedHashMap<String,String>();
		for (Text t : values) {
			String str = t.toString();
			// 共现矩阵A
			if (str.startsWith("A")) {
				String[] split = str.split(":");
				mapA.put(split[1],split[2]);
			}
			// 评分矩阵B
			else {
				if (flag) {
					String[] split = str.split("\t");
					String userId = split[0].split(":")[1];
					String score = "";
					for (int i = 1; i < split.length; i++) {
						score += split[i] + "\t";
					}
					mapB.put(userId, score);
				}
			}
		}
		flag = false;

		for (Entry<String, String> e : mapB.entrySet()) {
			int flag1 = 0;
			String key = e.getKey();
			float value = 0;
			String str = e.getValue();
			String[] split = str.split("\t");
			for(String i : split) {
				String[] split2 = i.split(":");
				String item = split2[0];
				if (item.equals(inKey.toString())) {
					flag1 = 1;
				}
				float score = Float.parseFloat(split2[1]);
				// 匹配共现矩阵，计算求和
				if (mapA.get(item)!=null) {
					float count = Float.parseFloat(mapA.get(item));
					value += (count * score);
				}
			}
			
			if(flag1 != 1){
				context.write(new Text(key), new Text(inKey+"\t"+value));
			}
			else{
				context.write(new Text(key), new Text(inKey+"\t"+value+"\t"+"已评分"));
			}
		}
	}
}