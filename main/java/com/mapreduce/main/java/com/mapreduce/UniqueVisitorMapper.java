package main.java.com.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UniqueVisitorMapper extends Mapper<LongWritable, Text, Text, Text> {	
	
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)throws IOException, InterruptedException{
		String [] words = value.toString().split(",");
		context.write(new Text(words[1]), new Text(words[0]));
	}
}
