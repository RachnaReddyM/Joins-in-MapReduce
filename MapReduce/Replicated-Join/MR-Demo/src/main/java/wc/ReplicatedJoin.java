package wc;

import java.io.*;
import java.util.*;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

// Used URI API to implement file cache. Referenced: https://stackoverflow.com/questions/21239722/hadoop-distributedcache-is-deprecated-what-is-the-preferred-api
// Another reference: http://hadoop.apache.org/docs/stable/api/org/apache/hadoop/mapreduce/Job.html#addCacheFile%28java.net.URI%29


public class ReplicatedJoin extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(ReplicatedJoin.class);

	// Implemented Counter in Hadoop with reference to : https://diveintodata.org/2011/03/15/an-example-of-hadoop-mapreduce-counter/
	public static enum TRIANGLE_COUNTER {
		MAX_CARDINALITY,
		NO_OF_TRIANGLES
	};

	public static int maxId = 40000;

	// Map only implementation
	public static class ReplicatedJoinMapper extends Mapper<Object, Text, Text, Text> {

		private HashMap<String,Set<String>> userFollower = new HashMap<>();

		// Setup method will read the records from input file and create a HashMap
		// To be placed on each worker machine
		// If each record is in form of S1,D1 S1,D2, S1,D3
		// HashMap is as- S1,[D1,D2,D3]
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			URI[] localPaths = context.getCacheFiles();

			if(localPaths!=null && localPaths.length>0){
				File inputFile = new File("edges.csv");
				FileInputStream fis = new FileInputStream(inputFile);
				BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
				String line;

				// For each record
				while ((line = reader.readLine()) != null) {

					String[] users = line.split(",");
					String user1 = users[0];
					String user2 = users[1];
					if(Integer.parseInt(user1)<=maxId && Integer.parseInt(user2)<=maxId) {
						if (!userFollower.containsKey(user1)) {
							Set<String> nodes = new HashSet<>();
							nodes.add(user2);
							userFollower.put(user1, nodes);
						} else {
							Set<String> nodes = userFollower.get(user1);
							nodes.add(user2);
							userFollower.put(user1, nodes);
						}
					}


				}
			}

		}


		// Map function accesses the HashMap to find the Path2 and the triangle Path.
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			final StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			String user1 = itr.nextToken();
			String user2 = itr.nextToken();

			if ((Integer.parseInt(user1) < maxId) && (Integer.parseInt(user2) < maxId)) {

				if(userFollower.containsKey(user2)) {
					// get the nodes which user2 follows.
					// nodes contains the Path2 nodes. user1 -> user2 -> all values in nodes
					Set<String> nodes = userFollower.get(user2);
					// increment with number of Path2 lengths.
					context.getCounter(TRIANGLE_COUNTER.MAX_CARDINALITY).increment(nodes.size());

					for (String s : nodes) {
						if (userFollower.containsKey(s)) {

							// get the nodes which user2's followers follow.
							// i.e user1 -> user2 -> user3
							// get users followed by user3
							Set<String> nodes1 = userFollower.get(s);
							for (String s1 : nodes1) {
								// if user3 follows user1 then there is a triangle
								if (s1.equals(user1)){
									Text src = new Text(s);
									Text dest = new Text(user1);
									context.write(src,dest);

									// increment number of triangles
									context.getCounter(TRIANGLE_COUNTER.NO_OF_TRIANGLES).increment(1);
							}
							}
						}
					}
				}
			}
		}

	}



	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job1 = Job.getInstance(conf, "Replicated Join");
		job1.setJarByClass(ReplicatedJoin.class);
		final Configuration jobConf1 = job1.getConfiguration();
		jobConf1.set("mapreduce.output.textoutputformat.separator", "\t");

		job1.setMapperClass(ReplicatedJoinMapper.class);
		job1.setNumReduceTasks(0);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		// Input file added in cache
		job1.addCacheFile(new URI(args[0]+"/"+"edges.csv"));

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		job1.waitForCompletion(true);

		Counter maxCardinality = job1.getCounters().findCounter(TRIANGLE_COUNTER.MAX_CARDINALITY);
		logger.info("Maximum Cardinality : "+maxCardinality.getValue());

		Counter triangleCount = job1.getCounters().findCounter(TRIANGLE_COUNTER.NO_OF_TRIANGLES);
		logger.info("Total Triangle Count : "+triangleCount.getValue()/3);
		return 0;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new ReplicatedJoin(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}