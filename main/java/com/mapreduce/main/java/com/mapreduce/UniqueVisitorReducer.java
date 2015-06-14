package main.java.com.mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UniqueVisitorReducer extends Reducer<Text,Text, Text,IntWritable>{
	protected void reduce(Text key, Iterable<Text> value,Reducer<Text, Text, Text, IntWritable>.Context context)throws IOException, InterruptedException{
		Set <String> uniqueVisitors = new HashSet<String>();
		for (Text user : value){
			uniqueVisitors.add(user.toString());
			
		}
		
			context.write(key, new IntWritable(uniqueVisitors.size()));
	}
}
