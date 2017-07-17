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

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import es.unavarra.chi_bd.core.Mediator;
import es.unavarra.chi_bd.utils.ByteArrayWritable;
import es.unavarra.chi_bd.utils.FrequentSubsetWritable;

/**
 * Mapper class that increments the counter of occurrences of a given subset of antecedents
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public class FrequentSubsetsMapper extends Mapper<ByteArrayWritable, ByteArrayWritable, FrequentSubsetWritable, LongWritable>{
    
	private LongWritable one = new LongWritable(1);
	private byte[] antecedents;
	private byte[] antsSubset;
	private int[][] splitsIndices;
	private int[] splitsLength;
	private int i, j, numSplit;
	
	@Override
    public void map(ByteArrayWritable key, ByteArrayWritable value, Context context) throws IOException, InterruptedException {

		antecedents = key.getBytes();
		
		// Split the rule in subsets of antecedents
		for (numSplit = 0; numSplit < Mediator.getNumRuleSplits(); numSplit++){
		
			j = 0;
			antsSubset = new byte[splitsLength[numSplit]];
	        for (i = splitsIndices[numSplit][0]; i <= splitsIndices[numSplit][1]; i++){
	        	antsSubset[j] = antecedents[i];
	        	j++;
	        }
	        
	        /*
	    	 * Key: Subset of antecedents of the rule
	    	 * Value: 1 occurrence
	    	 */
	        context.write(new FrequentSubsetWritable(antsSubset,numSplit), one);
        
		}
        
    }
	
	@Override
	protected void setup(Context context) throws InterruptedException, IOException{

		super.setup(context);
		
		try {
			Mediator.setConfiguration(context.getConfiguration());
			Mediator.readLearnerConfiguration();
		}
		catch(Exception e){
			System.err.println("\nSTAGE 2: ERROR READING CONFIGURATION\n");
			e.printStackTrace();
			System.exit(-1);
		}
		
		splitsIndices = Mediator.getRuleSplitsIndices();
		splitsLength = new int[Mediator.getNumRuleSplits()];
		for (numSplit = 0; numSplit < Mediator.getNumRuleSplits(); numSplit++)
			splitsLength[numSplit] = splitsIndices[numSplit][1]-splitsIndices[numSplit][0]+1;
		
	}
    
}
