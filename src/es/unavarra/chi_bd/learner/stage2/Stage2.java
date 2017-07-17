/*
 * Copyright (C) 2014 Mikel Elkano Ilintxeta
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package es.unavarra.chi_bd.learner.stage2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import es.unavarra.chi_bd.core.Mediator;
import es.unavarra.chi_bd.utils.FrequentSubsetWritable;

/**
 * Models the second MapReduce that computes the most frequent subsets of antecedents
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public class Stage2 {
    
    /**
	 * Runs Stage 2
	 * @author Mikel Elkano Ilintxeta
	 * @param args application arguments
	 * @param conf configuration object
	 * @version 1.0
	 */
    public static void runStage2 () throws Exception {
        
    	Configuration conf = Mediator.getConfiguration();
        
        /*
         * Prepare and run the job
         */
        Job job = Job.getInstance(conf);

        job.setJarByClass(Stage2.class);
        job.setMapperClass(FrequentSubsetsMapper.class);
        job.setCombinerClass(FrequentSubsetsCombiner.class);
        job.setReducerClass(FrequentSubsetsReducer.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        /*SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);*/
        job.setOutputKeyClass(FrequentSubsetWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(Mediator.getHDFSLocation()+"/"+Mediator.getLearnerRuleBaseTmpPath()));
        FileOutputFormat.setOutputPath(job, new Path(Mediator.getHDFSLocation()+Mediator.getLearnerStage2OutputPath()));
        
        job.waitForCompletion(true);
        
    }
    
}

