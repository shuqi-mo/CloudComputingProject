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
