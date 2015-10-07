
package mapReduce;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class combiner {
	public static class Map extends
	Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] elements = line.split(",");

			context.write(new Text(elements[2]), new //2 and 4 //dname n amount
					IntWritable(Integer.parseInt(elements[4])));
		}
	}

	public static class Reduce extends
	Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			System.out.println("Reducer key ="+ key.toString());

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static class DPartitioner<K, V> extends Partitioner<K, V> {

		/** Use {@link Object#hashCode()} to partition. */
		public int getPartition(K key, V value,
				int numReduceTasks) {
			if (key.toString().equalsIgnoreCase("Para"))
				return 0;
			else if (key.toString().equalsIgnoreCase("metacin"))
				return 1;
			else if (key.toString().equalsIgnoreCase("Crocin"))
				return 2;
			else
				return 3;
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		//        conf.addResource("resources/core-site.xml"); //to run on eclipse
		//        conf.addResource("resources/hdfs-site.xml");
		//        conf.addResource("resources/mapred-site.xml");
		Job job = new Job(conf, "Drug Amount Spent Partiotioner");

		job.setJarByClass(combiner.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(4);
		job.setMapperClass(Map.class);
		
		job.setReducerClass(Reduce.class); 
		job.setCombinerClass(Reduce.class);
		job.setPartitionerClass(DPartitioner.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		//        FileInputFormat.addInputPath(job, new Path("/xyz/apat2331.dat"));
		//        FileOutputFormat.setOutputPath(job, new Path("/drugoutput_partitioner_v3"));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));


		job.waitForCompletion(true);
	}

}