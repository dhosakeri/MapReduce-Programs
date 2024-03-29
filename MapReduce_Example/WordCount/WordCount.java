import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class WordCount {
	
	public static class Map extends MapReduceBase implements 
	Mapper<LongWritable, Text, Text, IntWritable>{

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter report)
				throws IOException {
			// TODO Auto-generated method stub
			
			String line = value.toString();
			
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			while(tokenizer.hasMoreTokens()){
				  value.set(tokenizer.nextToken());
				  output.collect(value, new IntWritable(1));
						
			}
			
			
			
			
		}
	}
	
	public static class Reduce extends MapReduceBase implements
	Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter report)
				throws IOException {
			// TODO Auto-generated method stub
			
			int sum =0;
			
			while(values.hasNext()){
				sum=sum+values.next().get();
			}
				
			output.collect(key,new IntWritable(sum));
					
		}
	}
	
	public static void main (String[] args) throws Exception{
		
		
		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("wordcount");
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		org.apache.hadoop.mapred.FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		
	}
}
	



