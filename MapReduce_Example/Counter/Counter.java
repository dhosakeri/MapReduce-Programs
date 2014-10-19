import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class counter {
	
	public static enum MONTH{
		DEC,
		JAN,
		FEB
		
	};
	
	public static class Mymapper extends Mapper<LongWritable, Text, Text,Text>{
		private Text out = new Text();
		
		protected void map (LongWritable key ,Text value,Context context)throws IOException,InterruptedException{
			String line = value.toString();
			String[] strts = line.split(",");
			long lts= Long.parseLong(strts[1]);
			Date time = new Date(lts);
			@SuppressWarnings("deprecation")
			int m = time.getMonth();
			
			if(m==11){
				context.getCounter(MONTH.DEC).increment(10);
			}
			if(m==0){
				context.getCounter(MONTH.JAN).increment(20);	
			}
			if(m==1){
				context.getCounter(MONTH.FEB).increment(30);
				
			}
			out.set("success");
			context.write(out, out);
	  }
	 
		
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		
		Job job = new Job();
		job.setJarByClass(counter.class);
		job.setJobName("counter");
		job.setNumReduceTasks(0);
		job.setMapperClass(Mymapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
		Counters counters = job.getCounters();
		
		org.apache.hadoop.mapreduce.Counter c1 = counters.findCounter(MONTH.DEC);
		System.out.println(c1.getDisplayName()+" : "+c1.getValue());
		
		
	}
}
