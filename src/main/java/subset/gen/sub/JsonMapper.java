package subset.gen.sub;

/**
 * Hello world!
 *
 */
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
//import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.log4j.Logger;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class JsonMapper {
	private static CdxRecord record;
public static class SampleMapper extends Mapper<LongWritable, Text, NullWritable,Text > { 
//	private static long seed=0;
//	private static double number;
//	private String outValue;
	private final NullWritable	outKey = NullWritable.get();
	private int test_number;
//	private static Logger logger = Logger.getLogger(SampleMapper.class);
 
	private String index;
	
	private int error;
	private int total_docs;
	private String type;
	private String clusterName;
	private String hostname;
	private BulkRequestBuilder bulk;
	private long bulkBuilderLength;
	private BulkResponse bulkRes;
	TransportClient client;
	
	 @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
			
			index = "souza_popularurls";
			error = total_docs = 0;
		    type = "capture";
			clusterName = "esearch";
			hostname = "node09.ib";
			
		    Settings settings = ImmutableSettings.settingsBuilder()
					   .put("cluster.name", clusterName).build();
			
		 
		    int port = 9300; 
		    client = new TransportClient(settings);
	    	client.addTransportAddress(new InetSocketTransportAddress(hostname, port));
		    
	        bulkBuilderLength = 0;
		    bulk=client.prepareBulk().setRefresh(true);
	 
	 }
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		
		
//			StringTokenizer token = new StringTokenizer (value.toString());
//			String year = token.nextToken().substring(0, 4);
		  	GsonBuilder builder = new GsonBuilder();
        	Gson capture = builder.create();
//			MapWritable doc = new MapWritable();
//        	logger.debug(value);
         	record = new CdxRecord ();
         	try {
				record = parseLine (value.toString());
			} catch (Exception e) {
				return;
			}
        
         	if (record == null)
         		return;
         	
         	if (value.toString().contains("filedesc:/DE"))
            	return;
         	
//         	try {
//         		test_number=Integer.parseInt(record.getTs().substring(0, 4));
         	
         	IndexRequestBuilder indexRequestBuilder = client.prepareIndex(index,type).
         			setSource(capture.toJson(record))
     		        .setId(record.getOrig() + record.getTs());
         	
         	try {
         	   
         		indexRequestBuilder.execute().actionGet();
         	  }
         	 catch (  Exception e) {
         	    return;
         	  }
         	
        		bulk.add(client.prepareIndex(index,type).setSource(capture.toJson(record))
         		        .setId(record.getOrig() + record.getTs()));
         		        bulkBuilderLength++;
         		        
//         		       bulkRes = bulk.execute().actionGet();        
         		
         		        if ((bulkBuilderLength % 5000000) == 0)
         		        {
         		        	bulkRes = bulk.execute().actionGet();
         		        	
         		            if(bulkRes.hasFailures()){
         		            	
         		            	context.write(outKey, new Text (bulkRes.buildFailureMessage().toString()));
         			       
         		            }
         		            else
         		            {
         		            	total_docs ++;
         		           
         		            }
         		            bulk = client.prepareBulk().setRefresh(true);
//         		            bulkBuilderLength=0;
         		         	context.write(outKey, new Text (bulkRes.buildFailureMessage().toString()));
         		        }
         		        
         		        
         		
//         	} catch (Exception e) {
//         		return;
//         	}
         	
    /*     	Text jsonDoc = new Text ();
         	jsonDoc.set(record.toString());
         	context.write(outKey, jsonDoc);
         	/*
         	doc.put(new Text("ts"), new Text(record.getTs()));
         	doc.put(new Text("orig"), new Text(record.getOrig()));
         	doc.put(new Text("redirectUrl"), new Text(record.getRedirectUrl()));
         	doc.put(new Text("compressedsize"), new Text(record.getCompressedsize()));
         	doc.put(new Text("offset"), new Text(record.getOffset()));
         	doc.put(new Text("filename"), new Text(record.getFilename()));
         	context.write(outKey, doc);*/   
		}
	    
	
	
	@Override
	protected void cleanup(Context context) throws IOException,
	InterruptedException {
		
		
		if(bulk.numberOfActions() > 0){
			
			bulkRes = bulk.execute().actionGet();
			if(bulkRes.hasFailures()){
	            	
	            	context.write(outKey, new Text (bulkRes.buildFailureMessage().toString()));
		       
	            }
	            
	            bulk = client.prepareBulk().setRefresh(true);
		}
		
		
		
//		if (bulkBuilderLength > 0)
//			bulkRes = bulk.execute().actionGet();
//        bulk = client.prepareBulk().setRefresh(true);

/*		if ((bulkBuilderLength % 1) == 0)
	        {
	        	bulkRes = bulk.execute().actionGet();
	        	
	            if(bulkRes.hasFailures()){
	            	
	            	context.write(outKey, new Text (bulkRes.buildFailureMessage().toString()));
		       
	            }
	            else
	            {
	            	total_docs ++;
	           
	            }
	            bulk = client.prepareBulk().setRefresh(true);
	        } 
  /*       bulk = client.prepareBulk().setRefresh(true);
		  if(bulkRes.hasFailures()){
          	error++;
          	context.write(outKey, new Text (bulkRes.buildFailureMessage().toString()));
	       
          }
		  else
		  {
			  total_docs++;
		  }
		  context.write(outKey, new Text (error + " " + total_docs));
		  client.close();
	*/	  
}
}
private static class CdxRecord {
	
//	private String url;
	private String ts;
	private String orig;
//	private String mime;
//	private String rescode;
//	private String checksum;
	private String redirectUrl;
//	private String meta;
	private String compressedsize;
	private String offset;
	private String filename;
//	private String domain;
//	private String keywords;
//	private String date;
//	private boolean isPopDomain;

	public String getDomainName(String url) throws URISyntaxException {
	    URI uri = new URI(url);
	    String domain = uri.getHost();
	    return domain.startsWith("www.") ? domain.substring(4) : domain;
	}

//	public void setDomain(String dom) {
//		domain = dom;
//	}

	public  String getTs() {
		return ts;
	}
	public  void setTs(String timestamp) {
		ts = timestamp;
	}
	public String getOrig() {
		return orig;
	}
	public  void setOrig(String o) {
		orig = o;
	}
//	public String getMime() {
//		return mime;
//	}
//	public  void setMime(String mimetype) {
//		mime = mimetype;
//	}
	
//	public  void setRescode(String resc) {
//		rescode = resc;
//	}

	
	public String getRedirectUrl() {
		return redirectUrl;
	}
	public void setRedirectUrl(String redirect) {
		redirectUrl = redirect;
	}
	
	
	public String getCompressedsize() {
		return compressedsize;
	}
	public void setCompressedsize(String comp) {
		compressedsize = comp;
	}
	public String getOffset() {
		return offset;
	}
	public void setOffset(String offs) {
		offset = offs;
	}
	public String getFilename() {
		return filename;
	}
	public void setFilename(String file) {
		filename = file;
	}

}

private static CdxRecord parseLine(String line) throws URISyntaxException { 
	
	StringTokenizer matcher = new StringTokenizer(line); 
	
//	matcher.nextToken();
	//record.setUrl (matcher.nextToken());
//	System.out.println(line);
	//matcher.nextToken();
	
	if (!matcher.hasMoreTokens())
		return null;
//	matcher.nextToken();
	if (!matcher.hasMoreTokens())
		record.setTs("noTs");
	else
	record.setTs(matcher.nextToken());
	
	if (!matcher.hasMoreTokens())
		record.setOrig("noOrig");
	else
	record.setOrig(matcher.nextToken());
	
//	matcher.nextToken();
//	matcher.nextToken();
//	matcher.nextToken();
/*		
	if (!matcher.hasMoreTokens())
		record.setMime("noMime");
	else
	record.setMime (matcher.nextToken());*/
	//record.setDomain(record.getDomainName (record.getOrig()));
	
	//matcher.nextToken();
	//record.setMime(matcher.nextToken());

	//record.setRescode(matcher.nextToken());
	//matcher.nextToken();
	
	//record.setChecksum(matcher.nextToken());
	matcher.nextToken();
	if (!matcher.hasMoreTokens())
		record.setRedirectUrl("noRedirect");
	else
	record.setRedirectUrl(matcher.nextToken());
//	matcher.nextToken();
//	matcher.nextToken();
	//record.setMeta(matcher.nextToken());
	//matcher.nextToken();
	if (!matcher.hasMoreTokens())
		record.setCompressedsize("nocompressed");
	else
	record.setCompressedsize(matcher.nextToken());
	//matcher.nextToken();
	if (!matcher.hasMoreTokens())
		record.setOffset("nooffset");
	else
	record.setOffset(matcher.nextToken());
//	matcher.nextToken();
	if (!matcher.hasMoreTokens())
		record.setFilename("nofilename");
	else
	record.setFilename(matcher.nextToken());
	//matcher.nextToken();
	return record; 
}


public static void main(String[] args) throws IOException,
InterruptedException, ClassNotFoundException {


Path inputPath = new Path(args[0]);
Path outputDir = new Path(args[1]);

// Create configuration
Configuration conf = new Configuration(true);
//Configuration conf = new Configuration();
//conf.setMapOutputValueClass(Text.class);
// Create jobr
//Job job = new Job(conf, "WordCount");

Job job = Job.getInstance(conf);
job.setJarByClass(SampleMapper.class);

// Setup MapReduce
job.setMapperClass(SampleMapper.class);
job.setReducerClass(Reducer.class);
job.setNumReduceTasks(0);

// Specify key / value
job.setOutputKeyClass(NullWritable.class);

job.setOutputValueClass(Text.class);

// Input
FileInputFormat.addInputPath(job, inputPath);
//job.setInputFormatClass(KeyValueTextInputFormat.class);
job.setInputFormatClass(TextInputFormat.class);

// Output
FileOutputFormat.setOutputPath(job, outputDir);
//job.setOutputFormatClass(TextOutputFormat.class);
//job.setOutputFormatClass(EsOutputFormat.class);
//job.setOutputKeyClass(NullWritable.class);
//job.setMapOutputValueClass(MapWritable.class);
job.setOutputValueClass(Text.class);

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
