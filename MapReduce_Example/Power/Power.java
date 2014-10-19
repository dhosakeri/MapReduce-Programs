import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class power {
	
	public static class mymap extends MapReduceBase implements
	Mapper<LongWritable, Text,Text, FloatWritable>{
		
		float x;
		float n;

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, FloatWritable> output,
				Reporter reporter) throws IOException {
			// TODO Auto-generated method stub
			
			String str = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(str,",");
			while(tokenizer.hasMoreTokens()){
				String input1=tokenizer.nextToken();
				x=Float.parseFloat(input1);
				String input2=tokenizer.nextToken().trim();
				n =Float.parseFloat(input2);
			}
			
			float out=x;
			if(n>=0){
			while((n-1)!=0){
				out= x*out;
				n--;
				}
			}
			else{
				n=-n;
				while((n-1)!=0){
					out = x*out;
					n--;
					}
				out=1/out;
				}
				
			
			output.collect(value,new FloatWritable(out));
			
		}
		
	}
	
	public static void main(String[] args)throws IOException{
		
		JobConf conf = new JobConf(power.class);
		conf.setJobName("power vaues");
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(FloatWritable.class);
		
		conf.setMapperClass(mymap.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);

	}
	

}

