import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TwitterUserFollowers extends Configured implements Tool {



	public static class MapClass extends Mapper<LongWritable, Text,Text, LongWritable > {

		private LongWritable one = new LongWritable(1l);

		public void map (LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String rowValue = value.toString().trim();
			if (!rowValue.isEmpty()) {
				context.write(value, one);
			}			
		}
	}


	public static class PostProcessing extends Mapper<Text, LongWritable,Text, LongWritable > {

		public void map (Text key, LongWritable value, Context context)
				throws IOException, InterruptedException {
			
			// output only users having followers more than 50
			if ( Long.valueOf(value.toString()) > 50l ) {
				context.write(key, value);
			}
		}
	}

	public static void deletePreviousOutput(Configuration conf, Path path)  {

		try {
			FileSystem hdfs = FileSystem.get(conf);
			hdfs.delete(path,true);
		}
		catch (IOException e) {
			//ignore any exceptions
		}
	}

	public static Job generateAndConfigureJob(Path in, Path out, Configuration conf) throws Exception {

		deletePreviousOutput(conf, out);

		//set any configuration params here. eg to say that the key and value are comma
		//separated in the input data add:
		//conf.set  ("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ":");

		Job job = Job.getInstance(conf);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// Set the MapperClass
		ChainMapper.addMapper(job, MapClass.class, LongWritable.class, Text.class, Text.class, LongWritable.class, conf);

		// Combiner class 
		job.setCombinerClass(LongSumReducer.class);

		// Set the Reducer class, we are using the LongSumReducer from Hadoop MapReduce API
		ChainReducer.setReducer(job, LongSumReducer.class, Text.class, LongWritable.class, Text.class, LongWritable.class, conf);

		// Set the post processing step after Reducer to output only users having followers greater than 50
		ChainReducer.addMapper(job, PostProcessing.class, Text.class, LongWritable.class, Text.class, LongWritable.class, conf);


		job.setInputFormatClass(TextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setJarByClass(TwitterUserFollowers.class);

		return job;
	}

	@Override
	public int run(String[] args) throws Exception {
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);

		Configuration conf = this.getConf();

		Job job = generateAndConfigureJob(in, out, conf);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new TwitterUserFollowers(), args);
		System.exit(result);
	}






}
