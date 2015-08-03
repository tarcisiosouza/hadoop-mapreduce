package subset.gen.sub;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class App {
	
public static class SampleMapper extends Mapper<Object, Text, NullWritable,Text > { 

	private final NullWritable	outKey = NullWritable.get();
	private int rescode;
	private String url;
	private static HashMap<String,String> domainsCategories = new HashMap<String, String>();
	private static HashMap<String,Integer> domains = new HashMap<String, Integer>();
	
	 	@Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	    
	        
	        Path location2 = new Path("/user/souza/complement.txt");
		    FileSystem fileSystem2 = location2.getFileSystem(context.getConfiguration());
		    
			RemoteIterator<LocatedFileStatus> fileStatusListIterator2 = fileSystem2.listFiles(
		            new Path("/user/souza/complement.txt"), true);
	        
			while(fileStatusListIterator2.hasNext())
			{
		    	
				String line2;
		        LocatedFileStatus fileStatus = fileStatusListIterator2.next();
		        BufferedReader br=new BufferedReader(new InputStreamReader(fileSystem2.open(fileStatus.getPath())));		    
		        while ((line2 = br.readLine()) != null) 
	            {
		        	StringTokenizer matcher = new StringTokenizer(line2);
		        	String Key = matcher.nextToken();
		        	domainsCategories.put(Key,"1");
	            }
			}
			
	 }
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		String filename = fileSplit.getPath().getName();
		
		if (filename.contains("_SUCESS") || filename.contains("_index") || filename.contains("_masterindex"))
			return;
		
			StringTokenizer token = new StringTokenizer (value.toString());
			try {
			token.nextToken();
			token.nextToken();
			} catch (Exception e)
			{
				return;
				
			}
	
			try {
			url = token.nextToken();
			
			token.nextToken();
			} catch (Exception e)
			{
				return;
			}
	
			try {
			rescode = Integer.parseInt(token.nextToken());
			
			} catch (Exception e)
			{
				return;
			}

			if (rescode != 200)
				return;	
			
			try {	
			URL Url = new URL (url.toString());
			
			String Domain = Url.getHost();
			if (Domain.contains("www"))
	 	   	{
	 		   int index = Domain.indexOf(".");
	     	   Domain = Domain.substring(index+1,Domain.length());
	 	   	}
			
			
			if (!domainsCategories.containsKey(Domain))
				return;
			
				if (domains.containsKey(Domain))
				{
					int current = domains.get(Domain);
					current = current + 1;
					domains.put(Domain, current);
					
				}
				else
					domains.put(Domain, 1);
			
			context.write(outKey, value);
		}
		catch (Exception e)
		{
			return;
		}
	}
	
}
public static void main(String[] args) throws IOException,
InterruptedException, ClassNotFoundException {


Path inputPath = new Path(args[0]);
Path outputDir = new Path(args[1]);

// Create configuration
Configuration conf = new Configuration(true);
conf.setInt("yarn.nodemanager.resource.memory-mb", 58000);
conf.setInt("yarn.scheduler.minimum-allocation-mb", 3000);
conf.setInt("yarn.scheduler.maximum-allocation-mb", 58000);
conf.setInt("mapreduce.map.memory.mb", 3000);
conf.setInt("mapreduce.reduce.memory.mb", 6000);
conf.setInt("yarn.scheduler.minimum-allocation-mb", 3000);
conf.setInt("yarn.app.mapreduce.am.resource.mb", 6000);
conf.set("mapreduce.map.java.opts", "-Xmx2400m");
conf.set("mapreduce.reduce.java.opts", "-Xmx4800m");
conf.set("mapred.map.child.java.opts", "-Xmx2400m");
//-Xmx512m
conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx4800m");
//conf.setInt("mapreduce.task.io.sort.mb", 15);
conf.setInt("mapreduce.task.io.sort.mb", 1000);
// Create jobr
//Job job = new Job(conf, "WordCount");
Job job = Job.getInstance(conf);
job.setJarByClass(SampleMapper.class);

// Setup MapReduce
job.setMapperClass(SampleMapper.class);
job.setReducerClass(Reducer.class);
job.setNumReduceTasks(1);

// Specify key / value
job.setOutputKeyClass(NullWritable.class);
job.setOutputValueClass(Text.class);

// Input
FileInputFormat.addInputPath(job, inputPath);
job.setInputFormatClass(TextInputFormat.class);

// Output
FileOutputFormat.setOutputPath(job, outputDir);
job.setOutputFormatClass(TextOutputFormat.class);

// Delete output if exists
FileSystem hdfs = FileSystem.get(conf);
if (hdfs.exists(outputDir))
hdfs.delete(outputDir, true);

FileOutputFormat.setCompressOutput(job, true);
FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
// Execute job
int code = job.waitForCompletion(true) ? 0 : 1;
System.exit(code);

}

}
