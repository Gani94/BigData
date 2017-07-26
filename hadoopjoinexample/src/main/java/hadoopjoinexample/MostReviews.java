package hadoopjoinexample;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MostReviews {

public static class UserMap extends Mapper<LongWritable, Text, Text, Text>{
	
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//from business
			
			String delims = "^";
			String[] userData = StringUtils.split(value.toString(),delims);
			if (userData.length ==3) {//key is user id and value is his name
					context.write(new Text(userData[0]), new Text("name^"+userData[1]));
			}		
		}
	
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
		}
	}
	
public static class ReviewMap extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//from business
		String delims = "^";
		String[] reviewData = StringUtils.split(value.toString(),delims);
		
		if (reviewData.length ==4) {//key is user id and value is "rating" word
			String val = "rating";
			
				context.write(new Text(reviewData[1]), new Text(val));
		}		
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
	}
}



public static class ReviewReduce extends Reducer<Text,Text,Text,Text> {

	

	private Map<String, Double> countMap = new HashMap<String, Double>();


@Override
public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {
	
	Double count=0.0;
	String details="";
	String delims = "^";
	
	for (Text val : values) {
		
		String[] vals = StringUtils.split(val.toString(),delims);
		if(vals[0].equals("rating")){
			count++;
		}
		else{
			details = key.toString() +"^" + vals[1];//adding name to user id
		}
	
	}
	
	//context.write(key,new Text(sum + " " + count + " " + formatter.format(avg)));
	if(!details.equals(""))
	countMap.put(details, count);
}


@Override
protected void cleanup(Context context) throws IOException, InterruptedException {
	//System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
	
	
	Set<Entry<String, Double>> set = countMap.entrySet();
    List<Entry<String, Double>> list = new ArrayList<Entry<String, Double>>(set);
    Collections.sort( list, new Comparator<Map.Entry<String, Double>>()
    {
        public int compare( Map.Entry<String, Double> o1, Map.Entry<String, Double> o2 )
        {
            return (o2.getValue()).compareTo( o1.getValue() );
        }
    } );
    
    Iterator<Entry<String, Double>> li = list.iterator();
    int counter = 0;
    while(li.hasNext() && counter<10) {
    	Map.Entry<String,Double> entry=li.next();
    	//String[] idName = entry.getKey().split("^");
    	String delims = "^";
    	String[] vals = StringUtils.split(entry.getKey(),delims);
    	if(vals.length==2)
    		context.write(new Text(vals[0]), new Text(vals[1]));
    	else
    		context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
        
    	counter++;
    }
	
	
  
}






}


	public static void main(String[] args) throws Exception{
	
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = Job.getInstance(conf, "join");
		job.setJarByClass(MostReviews.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, UserMap.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, ReviewMap.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setReducerClass(ReviewReduce.class);
		job.setNumReduceTasks(1);
		
		String output = otherArgs[2];
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
}
	
}
