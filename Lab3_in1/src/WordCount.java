import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;


public class WordCount {
	public static class MyMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		
		public void map(LongWritable key, Text value, OutputCollector<Text,IntWritable> output,Reporter reporter) throws IOException{
			String line = value.toString();
			StringTokenizer st = new StringTokenizer(line);
			String s;
			Text word = new Text();
			while(st.hasMoreTokens())
			{
				s = st.nextToken();
				word.set(s);
				output.collect(word, one);
			}
		}
	}
	public static class MyReducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>
	{
		public void reduce(Text key, Iterator<IntWritable> value, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException
		{
			int s = 0;
			while(value.hasNext())
			{
				IntWritable i = value.next();
				s+=i.get();
			}
			IntWritable i = new IntWritable(s);
			output.collect(key, i);
		}
	}
	
	public static void main(String[] args) throws IOException {
		JobConf conf= new JobConf(WordCount.class);
		conf.setJobName("Word Count");
		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}

}
