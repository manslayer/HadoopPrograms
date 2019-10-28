import java.util.*;
import java.io.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
public class TotalSal {
	public static class MyMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,LongWritable>
	{
		public void map(LongWritable key, Text value, OutputCollector<Text,LongWritable> output, Reporter reporter) throws IOException
		{
			String[] a = value.toString().split("    ");
			long sal = Long.parseLong(a[3]);
			LongWritable sal1 = new LongWritable(sal);
			Text t = new Text(a[2]);
			output.collect(t, sal1);
		}
	}
	public static class MyReducer extends MapReduceBase implements Reducer<Text,LongWritable,Text,Text>
	{
		public void reduce(Text key,Iterator< LongWritable> value, OutputCollector <Text,Text> output,Reporter reporter) throws IOException
		{
			long s =0;
			int n=0;
			while(value.hasNext())
			{
				LongWritable i = value.next();
				s=s+i.get();
				n++;
			}
			String ou = "Total :"+s+"    "+"Average:"+(s/n);
			Text outpu = new Text(ou);
			output.collect(key, outpu);
		}
	}
	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(TotalSal.class);
		conf.setJobName("Total Salary");
		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf,new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}

}
