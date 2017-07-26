import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;




import twitter4j.GeoLocation;
import twitter4j.JSONException;
import twitter4j.JSONObject;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterAPP3 {

	public static void main(String[] args) throws IOException, JSONException {
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
			.setOAuthConsumerKey("KaXLe4UHmLGduHrZXZokBeOBV")
			.setOAuthConsumerSecret("D6Ox2Rpcv1HRODMOVZV7Csr4SooaIj2q5fnUwVFstPRKModRta")
			.setOAuthAccessToken("2954893362-j5EVOjOBQoClfEqlDnI1aWbfRZH6IBcskjsFbZY")
			.setOAuthAccessTokenSecret("YZuffR1ctjvhW3uyeyLjkOhTQ3k1ohgEhXGkJvOw0CkWf");
		
		Twitter twitter = new TwitterFactory(cb.build()).getInstance();
		  Query query = new Query(args[0]);
		  query.setSince(args[1]);
		  query.setUntil(args[2]);
		  query.setLang("en");
		  int numberOfTweets = Integer.parseInt(args[3]);
		  long lastID = Long.MAX_VALUE;
		  ArrayList<Status> tweets = new ArrayList<Status>();
		  while (tweets.size () < numberOfTweets) {
		    if (numberOfTweets - tweets.size() > 100)
		      query.setCount(100);
		    else 
		      query.setCount(numberOfTweets - tweets.size());
		    try {
		      QueryResult result = twitter.search(query);
		      
		      tweets.addAll(result.getTweets());
		      for (Status t: tweets) 
		        if(t.getId() < lastID) lastID = t.getId();

		    }

		    catch (TwitterException te) {
		      System.out.print("Couldn't connect: " + te);
		    }; 
		    query.setMaxId(lastID-1);
		  }

		  File file = new File(args[4]);
		  FileWriter fw = new FileWriter(file.getAbsoluteFile());
		  BufferedWriter bw = new BufferedWriter(fw);
		  for (int i = 0; i < tweets.size(); i++) {
		    Status t = (Status) tweets.get(i);
		    String msg = t.getText();
//		    JSONObject obj = new JSONObject();
//		    obj.put("tweet",msg);
//		    bw.write(obj.toString());
//		    Date time = t.getCreatedAt();
		    //write the msg to a text file and then upload the resultant file to hdfs. i dont whether to automate the differnt durations to get 6 files
		   
		    bw.write("$$$$$&&**" + msg);
			  bw.newLine();
		  }
		  System.out.println("File Downloaded");
		  bw.close();
	}
}
