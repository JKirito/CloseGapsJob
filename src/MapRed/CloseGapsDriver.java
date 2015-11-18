package MapRed;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import entities.CeldaWritable;

public class CloseGapsDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");

		job.setJarByClass(CloseGapsDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(CloseGapsMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(CloseGapsReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(CeldaWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		// TODO: specify input and output DIRECTORIES
		FileInputFormat.setInputPaths(job, new Path(
				"/home/pruebahadoop/Documentos/DataSets/monitores/outputSinOutliersLONGLAT/part-r-00000"));
		FileOutputFormat.setOutputPath(job, new Path("/home/pruebahadoop/Documentos/DataSets/monitores/CloseGaps"));

		if (!job.waitForCompletion(true))
			return;
	}

}
