# 基于MapReduce的ItemCF

## 1 背景介绍

### 1.1 ItemCF

协同过滤(Collaborative Filtering, CF)是一个经典的传统推荐算法，协同过滤算法可以协同大家的反馈、评价和意见一起对海量的信息进行过滤，从中筛选出目标用户可能感兴趣的信息。ItemCF是基于物品相似度进行推荐的协同过滤算法，其基本思想是给用户推荐的物品和他之前喜欢的物品相似，具体步骤如下：

1. 基于用户的历史评分数据，构建以用户（假设用户数为m）为列坐标，物品（假设物品数为n）为行坐标的`n*m`维的**评分矩阵**$A$
2. 基于评分矩阵构建`n*n`维的**共现矩阵**$B$，表示评分矩阵中不同物品的相似性，其中$B_{ij}$的计算方法为物品$i$与物品$j$同时在某个用户的历史评分中出现的次数
3. 用户$u$对物品$i$喜爱程度等于用户$u$对任一物品$j$的评分乘以物品$i$和物品$j$的相似度，令$j$取可选的所有其他物品编号，重复上一个计算步骤，并把结果累加起来。这里的计算相当于让共现矩阵$B$与评分矩阵$A$相乘，生成一个`n*m`维的矩阵作为最终的推荐结果

### 1.2 MapReduce

MapReduce是一个分布式运算程序的编程框架，可以在Hadoop集群上并发运行用户用统一计算框架编写的业务逻辑代码。MapReduce的开发分为Map阶段和Reduce阶段，Map阶段需要在主函数设置输入格式和重写框架的map()函数，Reduce阶段需要在主函数设置输出格式和重写框架的reduce()函数。

以作业代码中的第一个job任务为例，MapReduce开发的流程写在了注释中。

```java
// 创建job实例userScoreMatrix
Job userScoreMatrix = Job.getInstance(super.getConf(), JobMain.class.getSimpleName());
// 指定程序的main函数
userScoreMatrix.setJarByClass(JobMain.class);
// 第一步：设置输入类与输入路径
userScoreMatrix.setInputFormatClass(TextInputFormat.class);
TextInputFormat.addInputPath(userScoreMatrix, new Path("hdfs://master:9000/itemcf/small.csv"));
// 第二步：设置mapper类与输出类型，其中UserScoreMapper为重写的map()函数
userScoreMatrix.setMapperClass(UserScoreMatrixMapper.class);
userScoreMatrix.setMapOutputKeyClass(Text.class);
userScoreMatrix.setMapOutputValueClass(Text.class);
// 第三步：设置reducer类与输出类型，其中UserScoreReducer为重写的reduce()函数
userScoreMatrix.setReducerClass(UserScoreMatrixReducer.class);
userScoreMatrix.setOutputKeyClass(Text.class);
userScoreMatrix.setOutputValueClass(Text.class);
// 第四步：设置输出类与输出路径
userScoreMatrix.setOutputFormatClass(TextOutputFormat.class);
Path pathUserScore = new Path("hdfs://master:9000/itemcf/userScoreMatrix");
FileSystem fileSystem = pathUserScore.getFileSystem(super.getConf());
// 防止在多次运行程序时报hdfs文件已存在的错误
if (fileSystem.exists(pathUserScore)) {
	fileSystem.delete(pathUserScore, true);
}
TextOutputFormat.setOutputPath(userScoreMatrix, pathUserScore);
// 等待userScoreMatrix执行完毕再执行下一个job
userScoreMatrix.waitForCompletion(true);
```

## 2 实验过程

使用MapReduce开发ItemCF算法时，把整个流程分成四个步骤：构建评分矩阵，构建共现矩阵，矩阵相乘生成推荐结果与推荐结果排序。对每一个步骤，分别创建一个job实例化对象，并在JobMain类中完成调用Mapper类Reducer类以及输入输出格式的设置。

JobMain.java

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobMain extends Configured implements Tool {
	@Override
    public int run(String[] args) throws Exception {
		// 1.计算评分矩阵
		Job userScoreMatrix = Job.getInstance(super.getConf(), JobMain.class.getSimpleName());
		userScoreMatrix.setJarByClass(JobMain.class);
		userScoreMatrix.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(userScoreMatrix, new Path("hdfs://master:9000/itemcf/small.csv"));
		userScoreMatrix.setMapperClass(UserScoreMatrixMapper.class);
		userScoreMatrix.setMapOutputKeyClass(Text.class);
		userScoreMatrix.setMapOutputValueClass(Text.class);
		userScoreMatrix.setReducerClass(UserScoreMatrixReducer.class);
		userScoreMatrix.setOutputKeyClass(Text.class);
		userScoreMatrix.setOutputValueClass(Text.class);
		userScoreMatrix.setOutputFormatClass(TextOutputFormat.class);
		Path pathUserScore = new Path("hdfs://master:9000/itemcf/userScoreMatrix");
		FileSystem fileSystem = pathUserScore.getFileSystem(super.getConf());
		if (fileSystem.exists(pathUserScore)) {
			fileSystem.delete(pathUserScore, true);
		}
		TextOutputFormat.setOutputPath(userScoreMatrix, pathUserScore);
		userScoreMatrix.waitForCompletion(true);
		
		// 2.计算共现矩阵
		Job itemMatrix = Job.getInstance(super.getConf(), JobMain.class.getSimpleName());
		itemMatrix.setJarByClass(JobMain.class);
		itemMatrix.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(itemMatrix, pathUserScore);
		itemMatrix.setMapperClass(ItemCoOccurenceMapper.class);
		itemMatrix.setMapOutputKeyClass(Text.class);
		itemMatrix.setMapOutputValueClass(IntWritable.class);
		itemMatrix.setReducerClass(ItemCoOccurenceReducer.class);
		itemMatrix.setOutputKeyClass(Text.class);
		itemMatrix.setOutputValueClass(IntWritable.class);
		itemMatrix.setOutputFormatClass(TextOutputFormat.class);
		Path pathItem = new Path("hdfs://master:9000/itemcf/itemMatrix");
		if (fileSystem.exists(pathItem)) {
			fileSystem.delete(pathItem, true);
		}
		TextOutputFormat.setOutputPath(itemMatrix, pathItem);
		itemMatrix.waitForCompletion(true);
		
		// 3.共现矩阵A*评分呢矩阵B
		Job matrixMulti = Job.getInstance(super.getConf(), JobMain.class.getSimpleName());
		matrixMulti.setJarByClass(JobMain.class);
		matrixMulti.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(matrixMulti, pathItem);
		TextInputFormat.addInputPath(matrixMulti, pathUserScore);
		matrixMulti.setMapperClass(MatrixMultiplicationMapper.class);
		matrixMulti.setMapOutputKeyClass(Text.class);
		matrixMulti.setMapOutputValueClass(Text.class);
		matrixMulti.setReducerClass(MatrixMultiplicationReducer.class);
		matrixMulti.setOutputKeyClass(Text.class);
		matrixMulti.setOutputValueClass(Text.class);
		matrixMulti.setOutputFormatClass(TextOutputFormat.class);
		Path pathMatrix = new Path("hdfs://master:9000/itemcf/calMatrix");
		if (fileSystem.exists(pathMatrix)) {
			fileSystem.delete(pathMatrix, true);
		}
		TextOutputFormat.setOutputPath(matrixMulti, pathMatrix);
		matrixMulti.waitForCompletion(true);
		
		// 4.排序得到最终结果
		Job rank = Job.getInstance(super.getConf(), JobMain.class.getSimpleName());
		rank.setJarByClass(JobMain.class);
		rank.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(rank, pathMatrix);
		rank.setMapperClass(RankMapper.class);
		rank.setMapOutputKeyClass(FloatPair.class);
		rank.setMapOutputValueClass(Text.class);
		rank.setReducerClass(RankReducer.class);
		rank.setOutputKeyClass(Text.class);
		rank.setOutputValueClass(Text.class);
		Path pathRank = new Path("hdfs://master:9000/itemcf/rank");
		if (fileSystem.exists(pathRank)) {
			fileSystem.delete(pathRank, true);
		}
		TextOutputFormat.setOutputPath(rank, pathRank);
		rank.waitForCompletion(true);
		
		return 1;
	}
	
	public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
```

### 2.1 构建评分矩阵

这一步把用户历史评分的原始数据转换成一个`n*m`维的评分矩阵。

#### 输入

small.csv

| userId | ItemId | Ratings |
| :----: | :----: | :-----: |
|   1    |  101   |    5    |
|   1    |  102   |    3    |
|   1    |  103   |   2.5   |
|   2    |  101   |    2    |
|   2    |  102   |   2.5   |
|   2    |  103   |    5    |
|   2    |  104   |    2    |
|   3    |  101   |    2    |
|   3    |  104   |    4    |
|   3    |  105   |   4.5   |
|   3    |  107   |    5    |
|   4    |  101   |    5    |
|   4    |  103   |    3    |
|   4    |  104   |   4.5   |
|   4    |  106   |    4    |
|   5    |  101   |    4    |
|   5    |  102   |    3    |
|   5    |  103   |    2    |
|   5    |  104   |    4    |
|   5    |  105   |   3.5   |
|   5    |  106   |    4    |
|   6    |  102   |    4    |
|   6    |  103   |    2    |
|   6    |  105   |   3.5   |
|   6    |  107   |    4    |

#### Map

把每一条用户评分记录转换成键值对<用户id，物品id：评分>。

```java
public class UserScoreMatrixMapper extends Mapper<LongWritable,Text,Text,Text> {
	Text userId = new Text();
	Text itemprocess = new Text();
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] strArr =  Pattern.compile("[\t,]").split(value.toString());
        userId.set(strArr[0]);
        itemprocess.set(strArr[1] + ":" + strArr[2]);
        context.write(userId, itemprocess);
	}
}
```

#### Reduce

合并键值相同的键值对，以"\t"分隔value集合的不同元素，便于后面分割成数组。

```java
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
```

#### 输出结果

part-r-00000(1)

```
1	103:2.5	101:5	102:3
2	101:2	102:2.5	103:5	104:2
3	107:5	101:2	104:4	105:4.5
4	103:3	106:4	104:4.5	101:5
5	101:4	102:3	103:2	104:4	105:3.5	106:4
6	102:4	103:2	105:3.5	107:4
```

### 2.2 构建共现矩阵

这一步生成物品的共现矩阵，“共现”的意思是两个物品同时在用户已评分的物品集合中出现。

#### 输入

2.1的输出结果

#### Map

对评分矩阵中的每一行，物品两两间构建键值对<item1：item2，1>。

```java
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
```

#### Reduce

合并key值相同的键值对，累加value值。

```java
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
```

#### 输出结果

part-r-00000(2)

```
101:101	5
101:102	3
101:103	4
101:104	4
101:105	2
101:106	2
101:107	1
102:101	3
102:102	4
102:103	4
102:104	2
102:105	2
102:106	1
102:107	1
...
```

### 2.3 矩阵相乘生成推荐结果

这一步令共现矩阵（记为矩阵A）与评分矩阵（记为矩阵B）相乘，得到推荐结果矩阵。

#### 输入

2.1得到的评分矩阵和2.2得到的共现矩阵

#### Map

这里的难点在于MapReduce是分布式计算框架，不会按JobMain的输入文件顺序读取数据，所以Map阶段读取数据时需要标记该数据是共现矩阵还是评分矩阵。区分的依据是评分矩阵的value值有:，共现矩阵没有。打标记的方式是在输出键值对的value值中加一个A:或B:的前缀。

```java
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
```

#### Reduce

根据前缀区分这是共现矩阵还是评分矩阵的记录，分别存入哈希表mapA和mapB。遍历评分矩阵mapB，匹配共现矩阵mapA元素，相乘得到分数值。这里还需要特别标记用户之前是否已评价过该物品。

```java
public class MatrixMultiplicationReducer extends Reducer<Text, Text, Text, Text> {
	private Map<String,String> mapA = new LinkedHashMap<String,String>();
    private Map<String,String> mapB = new LinkedHashMap<String,String>();
	private boolean flag = true;	// 标记用户是否已评分
	
	@Override
	protected void reduce(Text inKey, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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
			
			if(flag1 != 1) {
				context.write(new Text(key), new Text(inKey+"\t"+value));
			}
			else {
				context.write(new Text(key), new Text(inKey+"\t"+value+"\t"+"已评分"));
			}
		}
	}
}
```

#### 输出结果

part-r-00000(3)

```
6	101	31.0
1	101	44.0	已评分
5	101	68.0	已评分
3	101	40.0	已评分
2	101	45.5	已评分
4	101	63.0	已评分
6	102	35.0	已评分
1	102	37.0	已评分
5	102	51.0	已评分
3	102	28.0
2	102	40.0	已评分
4	102	40.0
6	103	37.0	已评分
1	103	44.5	已评分
5	103	65.0	已评分
3	103	34.0
2	103	49.0	已评分
4	103	56.5	已评分
6	104	25.0
1	104	33.5
5	104	59.0	已评分
3	104	38.0	已评分
2	104	36.0	已评分
4	104	55.0	已评分
6	105	30.5	已评分
1	105	21.0
5	105	40.5	已评分
3	105	35.5	已评分
2	105	23.0
4	105	29.0
6	106	11.5
1	106	18.0
5	106	34.5	已评分
3	106	16.5
2	106	20.5
4	106	33.0	已评分
6	107	21.0	已评分
1	107	10.5
5	107	20.0
3	107	25.0	已评分
2	107	11.5
4	107	12.5
```

### 2.4 推荐结果排序

把上面得到的推荐结果按userId升序和用户评分降序的方式排列，并且把已评分的记录放在前面，这样后续就可以从没有评分的记录中挑选分数高的推荐给用户。这一步的难点在于要重写WritableComparable中的FloatPair类实现自定义排序。

```java
public class FloatPair implements WritableComparable<FloatPair> {
	private float first;
	private float second;
	
	public void set(float left, float right) {
		first = left;
		second = right;
	}
	
	public float getFirst() {
		return first;
	}
	public float getSecond() {
		return second;
	}
	
	public void readFields(DataInput in) throws IOException {
		first = in.readFloat();
		second = in.readFloat();
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeFloat(first);
		out.writeFloat(second);
	}

	public int compareTo(FloatPair arg) {
		// 升序
        if (first != arg.first) {
			return (first - arg.first > 0) ? 1 : -1;
		}
		// 降序
        else if (second != arg.second) {
			return (arg.second - second > 0) ? 1 : -1;
		}
		else return 0;
	}
}
```

#### 输入

2.3得到的推荐结果

#### Map

```java
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
```

#### Reduce

```java
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
```

#### 输出结果

part-r-00000(4)

```
1.0	103	44.5	已评分
1.0	101	44.0	已评分
1.0	102	37.0	已评分
1.0	104	33.5
1.0	105	21.0
1.0	106	18.0
1.0	107	10.5
2.0	103	49.0	已评分
2.0	101	45.5	已评分
2.0	102	40.0	已评分
2.0	104	36.0	已评分
2.0	105	23.0
2.0	106	20.5
2.0	107	11.5
3.0	101	40.0	已评分
3.0	104	38.0	已评分
3.0	105	35.5	已评分
3.0	103	34.0
3.0	102	28.0
3.0	107	25.0	已评分
3.0	106	16.5
4.0	101	63.0	已评分
4.0	103	56.5	已评分
4.0	104	55.0	已评分
4.0	102	40.0
4.0	106	33.0	已评分
4.0	105	29.0
4.0	107	12.5
5.0	101	68.0	已评分
5.0	103	65.0	已评分
5.0	104	59.0	已评分
5.0	102	51.0	已评分
5.0	105	40.5	已评分
5.0	106	34.5	已评分
5.0	107	20.0
6.0	103	37.0	已评分
6.0	102	35.0	已评分
6.0	101	31.0
6.0	105	30.5	已评分
6.0	104	25.0
6.0	107	21.0	已评分
6.0	106	11.5
```

## 3 实验总结

在完成这次大作业时，我对MapReduce的开发流程和ItemCF算法有了更深的理解。本来在把small.csv的结果跑出来后还打算跑movieLens的数据集ratings.csv，该数据集有100836条用户评分记录，但是在集群运行到矩阵相乘的job任务时，两台worker节点虚拟机都显示磁盘空间不足（MapReduce运行时会把相关数据存放到tmp文件中缓存），然后集群自动进入安全模式，把ResourceManager关闭掉，没有办法继续执行任务。因此，如果想要在hadoop运行较大的数据集，需要提高虚拟机配置。此外，我查阅了相关资料后，发现矩阵乘法的计算这一步可以被继续优化，思路是把其拆分成乘法和加法两个步骤。虽然这次大作业由于时间原因，并没有来得及尝试做这一步优化，今后如果有机会需要用MapReduce处理包含矩阵乘法步骤的实际问题时，可以尝试使用这种优化方式减少程序运行时占用的磁盘空间。

