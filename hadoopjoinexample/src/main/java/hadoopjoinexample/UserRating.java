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

public class UserRating {

public static class UserMap extends Mapper<LongWritable, Text, Text, Text>{
		String name;
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//from business
			
			String delims = "^";
			String[] userData = StringUtils.split(value.toString(),delims);
//			String[] nameParts = StringUtils.split(name," "); 
//			int length = nameParts.length;
			if (userData.length ==3) {
//				boolean contains = true;
//				for(int i=0;i<length;i++){
//					if(!userData[1].toLowerCase().contains(nameParts[i]))
//						contains = false;
//					}
				if(userData[1].toLowerCase().equals(name.toLowerCase())){
					context.write(new Text(userData[0]), new Text("name"));
				}
			
			}		
		}
	
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			name = config.get("name");
		}
	}
	
public static class ReviewMap extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//from business
		String delims = "^";
		String[] reviewData = StringUtils.split(value.toString(),delims);
		
		if (reviewData.length ==4) {
			String val = "rating"+"^"+reviewData[3];
			
				context.write(new Text(reviewData[1]), new Text(val));
		}		
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
	}
}



public static class ReviewReduce extends Reducer<Text,Text,Text,Text> {

	

//	private Map<String, Double> countMap = new HashMap<>();


@Override
public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {

	Double count=0.0;
	Double sum = 0.0;
	String details="";
	String delims = "^";
	/*Iterator<IntWritable> itr = values.iterator();
	while(itr.hasNext()){
		count++;
		sum += itr.next().get();
	}*/
	
	for (Text val : values) {
		
		String[] vals = StringUtils.split(val.toString(),delims);
		if(vals[0].equals("rating")){
			sum += Double.parseDouble(vals[1]);
			count++;
		}
		else{
			details = key.toString();
		}
	
	}
	Double avg=0.0;
	if(count!=0) 
		avg = ((Double)sum/(Double)count); 
	//context.write(key,new Text(sum + " " + count + " " + formatter.format(avg)));
	if(!details.equals(""))
		context.write(new Text(details), new Text(avg.toString()));
}



}


	public static void main(String[] args) throws Exception{
	
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		int length = otherArgs.length;
		String completeName = "";
		for(int i=3;i<length;i++){
			completeName+=otherArgs[i] + " ";
		}
		conf.set("name", completeName.trim());
		Job job = Job.getInstance(conf, "join");
		job.setJarByClass(UserRating.class);
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
