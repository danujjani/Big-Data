import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/* 
 by Samtha
*/
public class PairCountCalc{

	public static TreeSet<ResultPair> sortedTreePair = new TreeSet<ResultPair>();
	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable>
		{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
			{

			String[] terms = value.toString().split("\\s+");
			Text wordpairkey = new Text();
			LongWritable wordpairval = new LongWritable(1);
			for (String term : terms) 
			{
				if (term.matches("\\w+")) 
				{	
					String update_A = term.trim() + " " + "#"; 
					wordpairkey.set(update_A);// just to indicate this set pf keys is denominator
					context.write(wordpairkey,wordpairval);
				}
			}
			StringBuilder sb = new StringBuilder();
			
			for (int i = 0; i < terms.length-1; i++)  
			{
				if (terms[i].matches("\\w+") && terms[i + 1].matches("\\w+")) // checking to encure only //w-> [a-zA-Z_0-9] are considered rest all ignored
					{
						sb.append(terms[i]).append(" ").append(terms[i + 1]);
						wordpairkey.set(sb.toString());
						context.write(wordpairkey,wordpairval);
						sb.delete(0, sb.length());
					}

			}
		}// end of map function

		}// end of map class


	public static class Combiner extends Reducer<Text, LongWritable, Text, LongWritable>
		{

		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
			{
				long count = 0;
				LongWritable sumofval= new LongWritable();

				for (LongWritable val : values)
					{
						count = count + val.get();
					}
				sumofval.set(count);
				context.write(key, sumofval);
			}

		}


	
		public static class Reduce extends Reducer<Text, LongWritable, Text, Text> {

		private DoubleWritable totalCount = new DoubleWritable();
		private DoubleWritable relativeCount = new DoubleWritable();

		private HashMap<String,Double> hm = new HashMap<String,Double>();

		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {

			if (key.toString().split(" ")[1].equals("#")) {
				double sum_A =0.0;
				String A = key.toString().split(" ")[0];

					totalCount.set(0);
					sum_A= getTotalCount(values);
					totalCount.set(sum_A);


				hm.put(A,sum_A);
			} 


			else {
				String A = key.toString().split(" ")[0];

				double count = getTotalCount(values); // count A B pairs
				double countofA = hm.get(A);
				relativeCount.set((double) count / countofA);
				Double relativeCountD = relativeCount.get();

					if(relativeCountD !=1.0d)
						{

							sortedTreePair.add(new ResultPair(relativeCountD,count, key.toString()));
							if (sortedTreePair.size() > 100) { sortedTreePair.pollLast(); } // removing last as my tress is descending 


						}
					
			} // close of else */
			
		} // close of reduce method

	private double getTotalCount(Iterable<LongWritable> values) {
			double count = 0;
			for (LongWritable value : values) {
				count += value.get();
			}
			return count;
		}



     protected void cleanup(Context context) throws IOException, InterruptedException 
		  {
			while (!sortedTreePair.isEmpty()) 
					{
					   ResultPair pair = sortedTreePair.pollFirst();
					   context.write(new Text(pair.key), new Text(Double.toString(pair.relativeFrequency)));
					}
			}

	}
	
	
	
	public static void main(String[] args) throws Exception 
	{
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(PairCountCalc.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Combiner.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.waitForCompletion(true);




	}


	public static class ResultPair implements Comparable<ResultPair> {
		double relativeFrequency;
		double count;
		String key;
		String value;

		ResultPair(double relativeFrequency, double count, String key) {
			this.relativeFrequency = relativeFrequency;
			this.count = count;
			this.key = key;
			  
		}

		@Override
		public int compareTo(ResultPair resultPair) {
			
				if(this.relativeFrequency<=resultPair.relativeFrequency){
								
				return 1;
			} else {
				return -1;
			}
			
		}

}

}