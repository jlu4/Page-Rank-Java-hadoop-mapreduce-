package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double d = PageRankDriver.DECAY; // Decay factor
		/* 
		 * TODO: emit key:node+rank, value: adjacency list
		 * Use PageRank algorithm to compute rank from weights contributed by incoming edges.
		 * Remember that one of the values will be marked as the adjacency list for the node.
		 */
		Double rank = 0.0;		
		ArrayList<String> weights = new ArrayList<String>();	
		for (Text v : values){
			weights.add(v.toString());
		}		
		Double weight = 0.0;
		String adj_list = "";
		for (String s : weights){
			if (!s.contains("?")){
				weight = weight+Double.parseDouble(s);
			}else{
				adj_list = s.substring(1);
			}
		}		
		rank = (1-d)+d*weight;		
		context.write(new Text(key.toString()+"+"+rank.toString()), new Text(adj_list));
		
	}
}
