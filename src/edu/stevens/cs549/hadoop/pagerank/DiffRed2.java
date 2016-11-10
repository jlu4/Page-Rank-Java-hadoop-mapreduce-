package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffRed2 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Double diff_max = 0.0; 
		/* 
		 * TODO: Compute and emit the maximum of the differences
		 */
		int i = 0;
		
		for (Text t : values){
			if (i==0){
				diff_max= Double.parseDouble(t.toString());
				i++;
				continue;
			}
			
			Double temp = Double.parseDouble(t.toString());
			
			if (temp>diff_max){
				diff_max = temp;
			}
		}
		
		context.write(new Text(diff_max.toString()), new Text());
	}
}
