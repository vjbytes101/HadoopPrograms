package custominput;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyExecutor {
	
	public static void main(String[] arg) throws IOException,ClassNotFoundException,InterruptedException{
		if (arg.length != 2) {
		      System.err.println("Usage: <input path> <output path>");
		      System.exit(-1);
		}
		
		Job job = new Job();
	    job.setJarByClass(MyExecutor.class);
	    job.setJobName("CustomTest");
	    job.setNumReduceTasks(0);
	    job.setMapperClass(MyMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setInputFormatClass(MyInputFormat.class);
	    
	    FileInputFormat.addInputPath(job, new Path(arg[0]));
	    FileOutputFormat.setOutputPath(job, new Path(arg[1]));
	    
	    job.waitForCompletion(true);
	}
}
