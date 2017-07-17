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

package es.unavarra.chi_bd.learner.stage3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import es.unavarra.chi_bd.core.Mediator;
import es.unavarra.chi_bd.utils.ByteArrayWritable;

/**
 * Models the second MapReduce that computes rule weights and removes conflicts
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public class Stage3 {
    
    /**
	 * Runs Stage 3
	 * @author Mikel Elkano Ilintxeta
	 * @param args application arguments
	 * @param conf configuration object
	 * @version 1.0
	 */
    public static void runStage3 () throws Exception {
    	
    	Configuration conf = Mediator.getConfiguration();
        
    	/*
    	 * Prepare and run the job
    	 */
        Job job = Job.getInstance(conf);

        job.setJarByClass(Stage3.class);
        if (Mediator.useCostSensitive())
        	job.setMapperClass(RuleWeightsMapperCS.class);
        else
        	job.setMapperClass(RuleWeightsMapper.class);
        job.setReducerClass(RuleWeightsReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(ByteArrayWritable.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(Mediator.getHDFSLocation()+"/"+Mediator.getLearnerInputPath()));
        FileInputFormat.setMaxInputSplitSize(job, Mediator.computeHadoopSplitSize(
    		Mediator.getHDFSLocation()+"/"+Mediator.getLearnerInputPath(), Mediator.getHadoopNumMappers()));
        FileInputFormat.setMinInputSplitSize(job, Mediator.computeHadoopSplitSize(
    		Mediator.getHDFSLocation()+"/"+Mediator.getLearnerInputPath(), Mediator.getHadoopNumMappers()));
        FileOutputFormat.setOutputPath(job, new Path(Mediator.getHDFSLocation()+"/"+Mediator.getLearnerStage3OutputPath()));

        int numReducers = Math.round(((float)Mediator.getLearnerRuleBaseSize())
			/ ((float)Mediator.getHadoopNumRulesReducer()));
        if (numReducers < 1)
        	job.setNumReduceTasks(1);
        else
        	job.setNumReduceTasks(numReducers);
        
        conf.setLong("mapreduce.task.timeout", Mediator.getHadoopMaxMinsNoUpdate()*60000);
        
        job.waitForCompletion(true);
        
    }
    
}

