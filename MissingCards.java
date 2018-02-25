import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class MissingCards
{

public static class CardMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	    {	
    	IntWritable cardValue = new IntWritable();
		Text cardType = new Text();
        String input = value.toString();
        String[] inputSplit = input.split(",");
		cardType.set(inputSplit[0]);
		cardValue.set(Integer.parseInt(inputSplit[1]));

        context.write(cardType,cardValue);
		 }
    }
 


public static class CardReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{   
    
	public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException 
		{
    	ArrayList<Integer>  outLsit = new ArrayList<Integer>();
		IntWritable result = new IntWritable();

    	int sum = 0;
    	int num = 0;
    	for(IntWritable cardValue : value) 
			
			{
    			sum= sum + cardValue.get();
    			num = cardValue.get();
    			outLsit.add(num); // adding cardnumber of that card type to alist
			}
   
    		if(sum < 91)
				{
    				for (int i = 1;i <= 13;i++)
						{
    						if(!outLsit.contains(i))
							{					
								result.set(i);
								context.write(key, result);	}
    						}
    			}
    
			}   
	}


	public static void main(String[] args)  throws Exception
	{
	Configuration conf = new Configuration();
    Job job = new Job(conf, "MissingCardsProgram");

    job.setJarByClass(MissingCards.class);
    job.setMapperClass(CardMapper.class);
    job.setReducerClass(CardReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1); 


	}

}