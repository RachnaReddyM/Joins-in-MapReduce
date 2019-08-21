package wc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ReduceSideJoin extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(ReduceSideJoin.class);

	// Implemented Counter in Hadoop with reference to : https://diveintodata.org/2011/03/15/an-example-of-hadoop-mapreduce-counter/

	public static enum TRIANGLE_COUNTER {
		MAX_CARDINALITY,
		NO_OF_TRIANGLES
	};

	// to restrict the maximum user IDs to be considered
	private static final int maxId = 40000;

    // This mapper class emits key-value as
	// emit(Source,"Source Destination;F")
	// emit(Destination,"Source Destination;T")
	public static class UserFollowerMapper extends Mapper<Object, Text, Text, Text> {

		private Text fromVal = new Text();
		private Text toVal = new Text();
		private Text fromKey = new Text();
		private Text toKey = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {


			final StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			String user1 = itr.nextToken();
			String user2 = itr.nextToken();

			if ((Integer.parseInt(user1) < maxId) && (Integer.parseInt(user2) < maxId)) {
				fromKey.set(user1);
				toKey.set(user2);

				fromVal.set(user1 + " " + user2 + ";" + "F");
				toVal.set(user1 + " " + user2 + ";" + "T");
				context.write(fromKey,fromVal);
				context.write(toKey,toVal);


			}
		}
	}


    // This Reducer class performs a cross product on the values in
	// from list and to list
	// Input: UserId, ["Source1 Destination1;F","Source2 Destination2;T"]
	// Output: ("Destination1,Source2","from")
	// This is sent to a Mapper
	public static class UserFollowerReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			Text nextPath = new Text();
			Text fromText = new Text("from");
			logger.info("in the first reducer");

			List<String> list_F = new ArrayList<>();
			List<String> list_T = new ArrayList<>();

			// Values are split into two different lists based on "F" or "T"
			for(Text node: values){
				String[] splitStr = node.toString().split(";");
				if(splitStr[1].equals("F"))
					list_F.add(splitStr[0]);
				else if(splitStr[1].equals("T"))
					list_T.add(splitStr[0]);
			}
			for (String node : list_F)
			{
				String[] listA = node.split(" ");
				//String user11 = listA[0];
				String user12 = listA[1];
				//logger.info(user11);
				logger.info(user12);


				for(String node1 : list_T)
				{

						logger.info("in the if condition");
						String[] listB = node1.split(" ");
						String user21 = listB[0];
						String user22 = listB[1];

						nextPath.set(user12 + "," + user21);
						context.write(nextPath,fromText);
						// Cardinality is incremented as this path may possibly form a triangle
						context.getCounter(TRIANGLE_COUNTER.MAX_CARDINALITY).increment(1);

				}
			}
		}
	}


    // This Mapper class takes input from UserFollowerReducer and emits
	// emit("Destination1, Source2","from")
	public static class SourceTriangleMapper extends Mapper<Object, Text, Text, Text> {

		private Text fromKey = new Text();
		private Text direction = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			final StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
			fromKey.set(itr.nextToken());
			direction.set(itr.nextToken());
			context.write(fromKey,direction);

		}
	}


	// This Mapper task takes input from the file and emits
	// emit ("Source,Destination","to")

	public static class DestTriangleMapper extends Mapper<Object, Text, Text, Text> {

		private Text toKey = new Text();
		private Text direction = new Text("to");

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			final StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			String user1 = itr.nextToken();
			String user2 = itr.nextToken();

			if ((Integer.parseInt(user1) < maxId) && (Integer.parseInt(user2) < maxId)) {

				toKey.set(user1 + "," + user2);
				context.write(toKey,direction);
			}

		}
	}

	// This Reducer task takes input from SourceTriangleMapper and DestTriangleMapper
	// Input: ("Source,Destination",["from","to","from"])
	// Output: ("Source,Destination",No of triangles)
	// It increments the Triangle Counter value if the path exists in the input file
	public static class TriangleCountReducer extends Reducer<Text, Text, Text, Text> {
		//private static int count=0;
		@Override
		public void reduce(Text key, Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			Integer toNode = 0;
			Integer fromNode = 0;
			//
			for (Text t : values) {
				if (t.toString().contains("from"))
					fromNode++;
				if (t.toString().contains("to"))
					toNode++;

			}

			// the node(Source,Destination) must exist in the input file atleast once
			if (fromNode >= 1 && toNode >= 1) {
				Text val = new Text();
				val.set(fromNode.toString());
				context.write(key,val);
				// Increment the number of triangles.
				context.getCounter(TRIANGLE_COUNTER.NO_OF_TRIANGLES).increment(fromNode);
			}
		}

	}

		@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();

		// Job1 has a Mapper and a Reducer
		final Job job1 = Job.getInstance(conf, "Reduce Side Join1");

		// Job2 has 2 Mappers and a Reducer
		final Job job2 = Job.getInstance(conf, "Reduce Side Join2");

		job1.setJarByClass(ReduceSideJoin.class);
		job2.setJarByClass(ReduceSideJoin.class);

		final Configuration jobConf1 = job1.getConfiguration();
		jobConf1.set("mapreduce.output.textoutputformat.separator", "\t");

		job1.setMapperClass(UserFollowerMapper.class);
		job1.setReducerClass(UserFollowerReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		// Wait until Job1 is completed to print cardinality and start Job2

		if(!job1.waitForCompletion(true)){
			System.exit(1);
		}

		long cardinality = job1.getCounters().findCounter(TRIANGLE_COUNTER.MAX_CARDINALITY).getValue();
		logger.info("Maximum Triangles we can have : " + cardinality);

		Path input = new Path(args[0]);
		Path intermediate_output = new Path(args[1]);
		Path output = new Path("s3://mapreduce-course/output48000");

		//Path output = new Path("/home/rachna/cs6240/Assignment2/MapReduce/Reduce-Side-Join/MR-Demo/finalout");

		MultipleInputs.addInputPath(job2, intermediate_output, TextInputFormat.class, SourceTriangleMapper.class);
		MultipleInputs.addInputPath(job2, input, TextInputFormat.class, DestTriangleMapper.class);
		job2.setReducerClass(TriangleCountReducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job2, output);

		// Wait until Job2 has completed execution.
		job2.waitForCompletion(true);

		Counter triangleCount = job2.getCounters().findCounter(TRIANGLE_COUNTER.NO_OF_TRIANGLES);
		logger.info("Total Triangle Count : "+triangleCount.getValue()/3);
		return 0;



	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new ReduceSideJoin(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}