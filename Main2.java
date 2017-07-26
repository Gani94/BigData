

import java.io.IOException;


public class Main2 {

	public static void main(String[] args) throws Exception {
		
		String[] startDates = {"2016-11-25","2016-11-26","2016-11-27","2016-11-28","2016-11-29","2016-11-30"};
		String[] endDates =   {"2016-11-26","2016-11-27","2016-11-28","2016-11-29","2016-11-30","2016-12-01"};
		String[] inputs = new String[5];
		String query = "google pixel";
		String tweetCount = "200";
		for(int i=0;i<6;i++){
			inputs[0] = query;
			inputs[1] = startDates[i];
			inputs[2] = endDates[i];
			inputs[3] = tweetCount;
			inputs[4] = "output"+i+".txt";
			TwitterAPP3.main(inputs);
		}
		
//		for(int i=0;i<6;i++){
//			String[] input = {"output"+i+".txt","hdfs://cshadoop1/user/mxb152530/input/"+"output"+i+".txt"};
//			FileCopyWithProgress.main(input);
//		}
//		
//		String[] in = {"input","mxb152530"};
//		HashtagCounter.main(in);
	
	}
}
