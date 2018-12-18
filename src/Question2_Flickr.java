import java.io.IOException;
import java.util.HashMap;

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

import com.google.common.collect.MinMaxPriorityQueue;

public class Question2_Flickr {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		//Configurer les paramètre 
		String input = otherArgs[0];
		String output = otherArgs[1];
		conf.setInt("K",Integer.parseInt(otherArgs[2]));
		
		Job job = Job.getInstance(conf, "Question2_Flickr");
		job.setJarByClass(Question2_Flickr.class);
		
		// Configurer Mapper Classe 
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringAndInt.class);

		// Configurer Reducer Classe 
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringAndInt.class);
		
		// Configurer Input Path
		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);
		
		// Configurer Output Path
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Configurer Combiner Classe
		job.setCombinerClass(MyCombiner.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	// Ranger une fichier et le sorti en Map<K, StringAndInt>
	public static class MyMapper extends Mapper<LongWritable, Text, Text, StringAndInt>{
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
		{
			// Traiter les données par chaque ligne
			String line = value.toString();
			String[] words=line.split("\t");
			
			// Obtenir le pays par sa coordonnée si il existe
			Country country = Country.getCountryAt(Double.parseDouble(words[11]), Double.parseDouble(words[10]));
			Text outputKey;
			if (country != null) {
				outputKey = new Text(country.toString());
			} else {
				outputKey = new Text("__");
			}
			Text outputValue = new Text(words[8]);
			// Sortir le résultat en Map<K, StringAndInt>
			con.write(outputKey, new StringAndInt(outputValue));
		}
	}

	// Recevoir le résultat en entrée d eMyCombiner et le sortir en Map<K,V> Ou V comprends au max 3 éléments
	public static class MyReducer extends Reducer<Text, StringAndInt, Text, StringAndInt>
	{
		public void reduce(Text country, Iterable<StringAndInt> values, Context con) throws IOException, InterruptedException
		{
			/*for (StringAndInt v : values) {
				con.write(country, v);
			}*/
			// Prendre le paramètre "k"
			int k = con.getConfiguration().getInt("K", 3);
			// Configurer la maximum de l'element dans chaque queue 
			MinMaxPriorityQueue<StringAndInt> lst = MinMaxPriorityQueue.maximumSize(k).create();
			for (StringAndInt v : values) {
				lst.add(new StringAndInt(new Text(v.tag.toString()), v.count));
			}
			
			for (StringAndInt el: lst) {
				con.write(country, el);
			}
		}
	}
	
	// Recevoir le résultat en entrée de MyMapper et Calculer le somme de l'occurence de tag dans chaque StringAndInt en sortie <K,(mK,mV)>
	public static class MyCombiner extends Reducer<Text, StringAndInt, Text, StringAndInt> {
		public void reduce(Text country, Iterable<StringAndInt> values, Context con) throws IOException, InterruptedException
		{
			HashMap<String,Integer> m = new HashMap<String,Integer>();
			for (StringAndInt value: values) {
				String s = value.tag.toString();
				String[] tags = s.split(",");
				for (String tag: tags) {
					tag = java.net.URLDecoder.decode(tag,"UTF-8");
					if (! tag.equals("")) {
						if (m.get(tag) != null) {
							m.put(tag, m.get(tag)+1);
						} else {
							m.put(tag, 1);
						}
					}
				}
			}
			
			/*for (String k: m.keySet()) {
				con.write(new Text(country), new Text(k+":"+m.get(k).toString()));
			}*/
			
			
			for (String myKey: m.keySet()) {
				con.write(country, new StringAndInt(new Text(myKey),m.get(myKey)));
			}
		}
	}
}