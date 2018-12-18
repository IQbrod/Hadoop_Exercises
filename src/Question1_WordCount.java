import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question1_WordCount {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		
		Job job = Job.getInstance(conf, "Question1_WordCount");
		job.setJarByClass(Question1_WordCount.class);
		
		// Configurer Mapper Classe 
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// Configurer Reducer Classe 
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// Configurer Input Path
		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);
		
		// Configurer Output Path
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Configurer Combiner Classe et Le nombre de ReduceTask
		job.setCombinerClass(MyReducer.class);
		job.setNumReduceTasks(3);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	//Calculer l'occurences de chaque mot en sortie <K,1>
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
		{	
			String line = value.toString();
			// split les mots 
			String[] words=line.split(" ");
			for(String word: words )
			{
				// Ignorer le majuscule et miniscule et supprimer la poncuation
				Text outputKey = new Text(word.toUpperCase().trim().replaceAll("[.,']", ""));
				IntWritable outputValue = new IntWritable(1);
				con.write(outputKey, outputValue);
			}
		}
	}

	//Recevoir le resultat en entrée de MyMapper et Calculer le somme de l'occurence de chaque mot en sortie <K,V>
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
		{
			int sum = 0;
			for(IntWritable value : values)
			{
				sum += value.get();
			}
			con.write(word, new IntWritable(sum));
		}
	}
}