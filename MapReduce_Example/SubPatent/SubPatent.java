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
import org.apache.hadoop.mapred.TextOutputFormat;;




public class SubPatent {
	
	public static class Map extends MapReduceBase implements
	Mapper<LongWritable,Text,Text,Text>{
		
		Text k = new Text();
		Text v = new Text();

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line," ");
			String kv = tokenizer.nextToken();
			k.set(kv);
			String vv = tokenizer.nextToken();
			v.set(vv);
			output.collect(k,v);
			
		}
		
	}
	
	public static class Reduce extends MapReduceBase implements
	Reducer<Text,Text,Text,IntWritable>{
		
		int cnt =0;

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			
			while(values.hasNext()){
				values.next();
				cnt++;
			}
			
			output.collect(key, new IntWritable(cnt));
			
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		JobConf conf = new JobConf(SubPatent.class);
		conf.setJobName("subpatent");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		
		conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		org.apache.hadoop.mapred.FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
	}
}

