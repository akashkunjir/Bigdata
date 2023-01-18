import java.util.*;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
public class FB{
	public static class InsightMapper extends MapReduceBase implements 
	Mapper< Object,Text, Text,	IntWritable> {
		boolean flag = false;

		// Map function
		public void map(Object key, Text value,OutputCollector<Text, IntWritable> output,
			Reporter reporter) throws IOException{
			String line[] = value.toString().split(",",10);
			if(flag){
				String type = line [1];
				String date = line [2];
				int likes = Integer.parseInt(line[6]);
				if(date.startsWith("2") && date.contains("2018") && type.equals("video"))
					output.collect(new Text("Likes"),new IntWritable(likes));
		}

		flag = true;
		}
	}
		// Reducer class
	public static class InsightReducer extends MapReduceBase implements Reducer
	< Text,IntWritable, Text,IntWritable >{
			// Reduce Function
			public void reduce(Text key,Iterator <IntWritable> values,
				OutputCollector<Text,IntWritable> output,Reporter reporter) throws IOException{
			int add=0;
			while(values.hasNext()){
				add = add + values.next().get();
			}
			output.collect(key,new IntWritable(add));
		}
	}


	public static void main(String args[]) throws Exception
	{
		JobConf conf = new JobConf(FB.class);
		conf.setJobName("Likes Insight");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(InsightMapper.class);
		conf.setReducerClass(InsightReducer.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf,new Path(args[0]));
		FileOutputFormat.setOutputPath(conf,new Path(args[1]));
		JobClient.runJob(conf);
	}
}