import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;


public class MaxTemp {
	
	public static class Map extends MapReduceBase implements
	Mapper<LongWritable, Text, IntWritable, IntWritable>{
		
		//Text k =new Text();


		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, IntWritable> output, Reporter report)
				throws IOException {
			
		
			// TODO Auto-generated method stub
			
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line," ");
			
			while(tokenizer.hasMoreTokens()){
				String year =tokenizer.nextToken();
				int k = Integer.parseInt(year);
				String temp =tokenizer.nextToken().trim();
				
                int v= Integer.parseInt(temp);
				
				output.collect(new IntWritable(k), new IntWritable(v));
				
			
		}
		
	}

   }
	
	
	
	public static class Reduce extends MapReduceBase implements
	Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
		
		
		

		@Override
		public void reduce(IntWritable key, Iterator<IntWritable> values,
				OutputCollector<IntWritable, IntWritable> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			int tempmax=0;
			while(values.hasNext()){
				int temp = values.next().get();
				if(tempmax<temp){
					tempmax=temp;
					
				}
				
			}
			
			output.collect(key, new IntWritable(tempmax));
			
		}
		
		
	}
	
	public static void main (String[] args) throws Exception{
		
		JobConf conf = new JobConf(MaxTemp.class);
		conf.setJobName("MaxTemp");
		
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(IntWritable.class);
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		org.apache.hadoop.mapred.FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		
	}
}

