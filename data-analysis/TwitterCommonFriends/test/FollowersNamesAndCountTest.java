

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import followers.FollowersNamesAndCount;
import followers.TwitterUserWritable;

public class FollowersNamesAndCountTest {

	MapDriver<LongWritable, Text, Text, TwitterUserWritable> mapDriver;
	ReduceDriver<Text, TwitterUserWritable, Text, TwitterUserWritable> combineDriver;
	ReduceDriver<Text, TwitterUserWritable, Text, TwitterUserWritable> mapReducer;



	@Before
	public void setUp() throws Exception {
		mapDriver = MapDriver.newMapDriver(new FollowersNamesAndCount.MapClass());
		combineDriver = ReduceDriver.newReduceDriver(new FollowersNamesAndCount.Combiner());
		mapReducer = ReduceDriver.newReduceDriver(new FollowersNamesAndCount.Reduce());
	}


	@Test
	public void testMapper() throws IOException{
		mapDriver.addInput(new LongWritable(1),new Text("Rewanth	Manikanta"));
		mapDriver.addOutput(new Text("Manikanta"), new TwitterUserWritable("Rewanth",1));
		mapDriver.runTest();
	}
	
	@Test
	public void testCombiner() throws IOException {
		TwitterUserWritable aw1 = new TwitterUserWritable("Dan",1);
		TwitterUserWritable aw2 = new TwitterUserWritable("Nolean",1);
		TwitterUserWritable aw3 = new TwitterUserWritable("Rewanth",1);
		TwitterUserWritable aw4 = new TwitterUserWritable("Ankil",1);
		List<TwitterUserWritable> values = new ArrayList<TwitterUserWritable>();
		values.add(aw1);
		values.add(aw2);
		values.add(aw3);
		values.add(aw4);
		combineDriver.addInput(new Text("Manikanta"), values);
		combineDriver.addOutput(new Text("Manikanta"), new TwitterUserWritable("Dan Nolean Rewanth Ankil ",4));
		combineDriver.runTest();
	}
	
	@Test
	public void testReducer() throws IOException {
		TwitterUserWritable aw1 = new TwitterUserWritable("Dan Nolean Rewanth ",3);
		TwitterUserWritable aw2 = new TwitterUserWritable("Datla ",1);
		List<TwitterUserWritable> values = new ArrayList<TwitterUserWritable>();
		values.add(aw1);
		values.add(aw2);
		mapReducer.addInput(new Text("Manikanta"), values);
		mapReducer.addOutput(new Text("Manikanta"), new TwitterUserWritable("[Dan Nolean Rewanth Datla ]",4));
		mapReducer.runTest();
	}

}
