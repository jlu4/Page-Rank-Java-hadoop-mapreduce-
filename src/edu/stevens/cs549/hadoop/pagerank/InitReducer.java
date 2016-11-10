package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/* 
		 * TODO: Output key: node+rank, value: adjacency list
		 */
		String k = key.toString();
		
		String rank = "1";
		
		k = k +"+"+rank;
		
		String outputValue="";
		
		for (Text value : values){
			outputValue = outputValue + value.toString()+"0";
		}
		context.write(new Text(k), new Text(outputValue.substring(0, outputValue.lastIndexOf("0"))));
	}
}
