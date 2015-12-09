package followers;

import java.io.IOException;
import java.util.Iterator;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * This is a MapReduce code to find the Twitter followers count and their names 
 * from the given data-set collected using the Twitter Scrapper code written in JavaScript.
 * 
 * @author Revanth Varma Datla
 *
 */

public class FollowersNamesAndCount  {

	public static class MapClass extends Mapper<LongWritable, Text,Text, TwitterUserWritable > {		
		
		/**
		 * The Mapper takes the random key and each row as an value for input and 
		 * outputs the follower name(X) as key and the user who follows(X) and count(1) as a value.
		 * The output value is stored in a user-defined data-type called TwitterUserWritable 
		 * which stores the user-name and count.
		 * e.g. Input: 1	Revanth	mani
		 * 		Output:mani	(Revanth,1)
		 * Here the representation (Revanth,1) is stored as an new TwitterUserWritable object 
		 */
		public void map (LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String sValue = value.toString();
			String[] values = sValue.trim().split("\t");

			context.write(new Text(values[1]),new TwitterUserWritable(values[0], 1));			
		}
	}

	public static class Combiner extends Reducer<Text, TwitterUserWritable,Text,TwitterUserWritable> {

		/**
		 * The combiner class is used to avoid shuffling large data.
		 * e.g. Input: mani	(Revanth,1)
		 * 			   mani (Dan,1)
		 * 		Output: mani	(Revanth Dan,2)
		 */
		public void reduce(Text key, Iterable<TwitterUserWritable> values, Context context)
				throws IOException, InterruptedException {

			Iterator<TwitterUserWritable> it = values.iterator();
			StringBuilder userFollowers  = new StringBuilder();
			int noOfFollowers=0;
			while (it.hasNext()) {
				TwitterUserWritable tuw = it.next();
				userFollowers.append(tuw.getUser()).append(" ");
				noOfFollowers += tuw.getNoOfFollowers(); 
			}
			context.write(key, new TwitterUserWritable(userFollowers.toString(),noOfFollowers));			
		}
	}

	public static class Reduce extends Reducer<Text, TwitterUserWritable,Text,TwitterUserWritable> {
		/**
		 * The reduce class will sum up all the followers count.
		 * e.g. Input: mani (Revanth Dan,2)
		 * 			   mani	(Noelan Ankil,2)
		 * 		Output: mani (Revanth Dan Noelan Ankil,4)
		 */
		public void reduce(Text key, Iterable<TwitterUserWritable> values, Context context)
				throws IOException, InterruptedException {

			Iterator<TwitterUserWritable> it = values.iterator();
			StringBuilder userFollowers  = new StringBuilder("[");
			int noOfFollowers=0;
			while (it.hasNext()) {
				TwitterUserWritable tuw = it.next();
				userFollowers.append(tuw.getUser());
				noOfFollowers += tuw.getNoOfFollowers();
			}
			userFollowers.append("]");
			context.write(key, new TwitterUserWritable(userFollowers.toString(),noOfFollowers));
		}
	}

	public static class PostProcessing extends Mapper<Text, TwitterUserWritable,Text, TwitterUserWritable > {

		/**
		 * Post-Processing class outputs only the followers with count > 50 
		 * follower name, count and list of users who are following as a tab separating fields.
		 * e.g. Input: 
		 */
		public void map (Text key, TwitterUserWritable value, Context context)
				throws IOException, InterruptedException {


			if ( value.getNoOfFollowers() > 1 ) {
				context.write(key, value);
			}
		}
	}

	/**
	 * Deletes the hdfs output directory before the Job starts.
	 * @param conf - The mapreduce configuration
	 * @param path - output directory path.
	 */
	public static void deletePreviousOutput(Configuration conf, Path path)  {

		try {
			FileSystem hdfs = FileSystem.get(conf);
			hdfs.delete(path,true);
		}
		catch (IOException e) {
			//ignore any exceptions
		}
	}

	public static void main(String[] args) throws Exception {
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		
		Configuration conf = new Configuration();

		deletePreviousOutput(conf, out);

		//set any configuration params here. eg to say that the key and value are comma
		//separated in the input data add:
		//conf.set  ("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

		Job job = Job.getInstance(conf);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TwitterUserWritable.class);

		// Set the MapperClass
		ChainMapper.addMapper(job, MapClass.class, LongWritable.class, Text.class, Text.class, TwitterUserWritable.class, conf);

		// Combiner class 
		job.setCombinerClass(Combiner.class);

		// Set the Reducer class.
		ChainReducer.setReducer(job, Reduce.class, Text.class, TwitterUserWritable.class, Text.class, TwitterUserWritable.class, conf);

		// Set the post processing step after Reducer to output only users having followers greater than 50
		ChainReducer.addMapper(job, PostProcessing.class, Text.class, TwitterUserWritable.class, Text.class, TwitterUserWritable.class, conf);

		job.setInputFormatClass(TextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setJarByClass(FollowersNamesAndCount.class);
		job.submit();

	}




}
