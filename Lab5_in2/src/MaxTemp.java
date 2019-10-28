import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class MaxTemp{
	public static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable>
	{
		public void map(LongWritable key, Text value, OutputCollector<Text,LongWritable> output, Reporter reporter) throws IOException
		{
			String s = value.toString();
			String[] a = s.split("	");
			Text t = new Text(a[0]);
			LongWritable l = new LongWritable(Long.parseLong(a[1]));
			output.collect(t,l);
		}
	}
	public static class MyReducer extends MapReduceBase implements Reducer<Text,LongWritable,Text,LongWritable>
	{
		String maxyear="";
		long maxtemp =0;
		public void reduce(Text key,Iterator<LongWritable> value, OutputCollector<Text,LongWritable> output,Reporter reporter) throws IOException
		{	
			while(value.hasNext())
			{
				LongWritable i = value.next();
				if(i.get()>maxtemp)
				{
					maxtemp = i.get();
					maxyear = key.toString();
				}
			}
			output.collect(new Text(maxyear), new LongWritable(maxtemp));
		}
		
	}
	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(MaxTemp.class);
		conf.setJobName("Max Temp");
		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);

	}

}
