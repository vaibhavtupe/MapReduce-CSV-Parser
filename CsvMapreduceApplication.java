package com.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.springframework.boot.autoconfigure.SpringBootApplication;


/**
 * @author vaibhv tupe
 * This is Mapreduce demo application class, which cleans the input CSV
 * file and gives the output as cleaned CSV file written on HDFS also writes
 * total error count in another file on HDFS.
 *
 */
@SpringBootApplication
public class CsvMapreduceApplication {

	public static class CSVCleanMapper
	extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable();
		private Text word = new Text();
		final String DELIMITER = ",";
		
		/* 
		 * Map method of Mapper class, takes input as one line of input file at a time 
		 * and cleans it. Gives output as key-value pair, where key is cleaned line and 
		 * value is number of errors in that line.
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringBuilder sb = new StringBuilder();
			String[] tokens = value.toString().split(DELIMITER);
			int totalError=0;
			for(int i=0; i< tokens.length;i++){
				String token = tokens[i];
				int totalLength = token.length();
				token=token.trim();
				token = token.replaceAll("^\"|\"$", "");
				totalError+=(totalLength-token.length());
				sb.append(token);
				if(i<tokens.length-1)
					sb.append(DELIMITER);  
			}
			word.set(sb.toString());
			one.set(totalError);
			context.write(word, one);
		}
	}

	public static class CSVCleanReducer
	extends Reducer<Text,IntWritable,Text,IntWritable> {

		private IntWritable result = new IntWritable();
		private MultipleOutputs<Text, IntWritable> mos;
		private int sum=0;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, IntWritable>(context);
		}

		/* Reduce method of reducer class, adds error count and also 
		 * writes the cleaned input CSV file lines to the new file 
		 * on HDFS.
		 */
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {

			for (IntWritable val : values) {
				sum += val.get();
			}
			mos.write("clean", key, NullWritable.get());
		}

		/* cleanup method writes the total error count 
		 * to the new file on the HDFS.
		 */
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			result.set(sum);
			mos.write("totalerrors", result, NullWritable.get());
			mos.close();
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "CSV cleaner");

		job.setJarByClass(CSVCleaner.class);
		job.setMapperClass(CSVCleanMapper.class);
		job.setCombinerClass(CSVCleanReducer.class);
		job.setReducerClass(CSVCleanReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class); 

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		MultipleOutputs.addNamedOutput(job, "clean", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "totalerrors", TextOutputFormat.class, IntWritable.class, NullWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
