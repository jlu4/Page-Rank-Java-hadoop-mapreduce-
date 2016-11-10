package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); 
		String[] sections = line.split("\t"); 

		if (sections.length > 2)
		{
			throw new IOException("Incorrect data format");
		}
		if (sections.length != 2) {
			return;
		}
		
		/* 
		 * TODO: emit key: adj vertex, value: computed weight.
		 * 
		 * Remember to also emit the input adjacency list for this node!
		 * Put a marker on the string value to indicate it is an adjacency list.
		 */		
		String[] adjlist = sections[1].split(" "); 	
		String[] node_rank = sections[0].split("[+]");
		Double weight = Double.parseDouble(node_rank[1])/adjlist.length;	
		for (String a : adjlist){
			context.write(new Text(a), new Text(weight.toString())); 
		}		
		context.write(new Text(node_rank[0]), new Text("?"+sections[1]));

	}

}
