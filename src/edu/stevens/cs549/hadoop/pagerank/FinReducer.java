package edu.stevens.cs549.hadoop.pagerank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinReducer extends Reducer<DoubleWritable, Text, Text, Text> {
	
	private HashMap<String, String> map;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException{
		if (context.getCacheFiles()!=null && context.getCacheFiles().length>0){
			URI[] files = context.getCacheFiles();
			Path path = new Path(files[0]);
			InputStream in = null;
			BufferedReader buff = null;
			map = new HashMap<String, String>();
			if (path!=null){
				FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"),new Configuration());
				
				FileStatus[] status=fs.listStatus(path);
				
				
				for(int i =0; i<status.length;i++){
						Path p = new Path(status[i].getPath().toString());
						
						in = fs.open(p);
						buff = new BufferedReader(new InputStreamReader(in));
						String str =null;
						while((str=buff.readLine())!=null){
							
							String[] tmp = str.split(": ");
							String key = tmp[0];
							String value = tmp[1];
							map.put(key, value);
					    }
				}
			} 
		} else{
			throw new IOException("catch file can not find.");
		}
		
		
	}
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {
		/* 
		 * TODO: For each value, emit: key:value, value:-rank
		 */
		Double rank = -Double.parseDouble(key.toString());
		for (Text t : values){
		//    context.write(new Text(t.toString()), new Text(rank.toString()));
		      if (map.containsKey(t.toString())){
				String outputKey = map.get(t.toString());
		//		System.out.println(outputKey);
		//		System.out.println(rank.toString());
				context.write(new Text(outputKey), new Text(rank.toString()));
				map.remove(t);
			}
		}
		map.clear();
	}
	
}
