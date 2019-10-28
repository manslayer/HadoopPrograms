import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.*;
public class NoOfProducts {
	public static class MyMapper extends MapReduceBase implements Mapper<LongWritable,Text, Text ,IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		public void map(LongWritable key, Text value, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException
		{
			String s = value.toString();
			String[] a = s.split(",");
			Text s1 = new Text(a[7]);
			output.collect(s1, one);
			
		}
		
	}
	public static class MyReducer extends MapReduceBase implements Reducer<Text,IntWritable, Text,IntWritable>
	{
		public void reduce(Text key, Iterator<IntWritable> value, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException
		{
			int s =0;
			while(value.hasNext())
			{
				IntWritable i = value.next();
				s = s+i.get();
			}
			IntWritable s1 = new IntWritable(s);
			output.collect(key, s1);	
		}
	}
	public static void main(String[] args) throws IOException {
		JobConf  conf = new JobConf(NoOfProducts.class);
		conf.setJobName("No of products from each country");
		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setInputFormat(TextInputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);

	}

}
