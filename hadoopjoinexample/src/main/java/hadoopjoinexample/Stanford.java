package hadoopjoinexample;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Stanford {

public static class BusinessMap extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//from business
			String delims = "^";
			String[] businessData = StringUtils.split(value.toString(),delims);
			
			if (businessData.length ==3) {
				
				if(businessData[1].toLowerCase().contains("stanford")){
					String val = "bizz"+"^"+businessData[1];
					context.write(new Text(businessData[0]), new Text(val));
				}
				
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
		String[] businessData = StringUtils.split(value.toString(),delims);
		
		if (businessData.length ==4) {
			String val = "rev"+"^"+businessData[1]+"^"+businessData[3];
			
				context.write(new Text(businessData[2]), new Text(val));
		}		
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
	}
}



public static class ReviewReduce extends Reducer<Text,Text,Text,Text> {

	


@Override
public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {

	Map<String, String> countMap = new HashMap<String, String>();
	String delims = "^";
	boolean fromStanford = false;
	for (Text val : values) {
		
		String[] vals = StringUtils.split(val.toString(),delims);
		if(vals[0].equals("rev")){//value is from review so add it to map. Assuming a user writes only one review per business
			countMap.put(vals[1], vals[2]);
		}
		else{//value is from business and we only wrote businesses from stanford hence this business is from stanford
			fromStanford = true;
		}
			
		
	}
	
	if(fromStanford){
		for(Map.Entry<String, String> entry: countMap.entrySet()){
			context.write(new Text(entry.getKey()), new Text(entry.getValue()));
		}
	}
	
	
}

}

	public static void main(String[] args) throws Exception{
	
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "join");
		job.setJarByClass(Stanford.class);
		
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, BusinessMap.class);
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
