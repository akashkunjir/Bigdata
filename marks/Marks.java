import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.StringTokenizer;
import java.io.IOException;

public class Marks {
	public static class MarksMapper extends Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context) 
		throws IOException, InterruptedException{

			String line = value.toString();
			StringTokenizer st = new StringTokenizer(line,",");
			String name = st.nextToken();
			int marks = Integer.parseInt(st.nextToken());
			String cls = st.nextToken();
			//if(marks >=60)
			if(cls.equals("TY"))
				context.write(new Text("Marks"),new IntWritable(marks));
		}
	}

	public static class MarksReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
		throws IOException, InterruptedException{

			FloatWritable avg = new FloatWritable();
			int sum = 0;
			int total = 0;
			for (IntWritable num: values){
				sum =sum + num.get();
				total++;
				}
			avg.set((float)sum/total);
			context.write(new Text("Average Marks: "),avg);
		}
	}

	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();

		Job job = new Job(conf, "Marks");

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(MarksMapper.class);
		job.setReducerClass(MarksReducer.class);		

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);			
	}
}
