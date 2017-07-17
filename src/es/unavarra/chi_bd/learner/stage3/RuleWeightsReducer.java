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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import es.unavarra.chi_bd.core.Mediator;
import es.unavarra.chi_bd.utils.ByteArrayWritable;

/**
 * Reducer class used to add the matching degrees of the classes and compute rule weights
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public class RuleWeightsReducer extends Reducer<IntWritable, MapWritable, ByteArrayWritable, FloatWritable> {

	/**
	 * Stores the rule base
	 */
	private byte[][] ruleBase; // Antecedents of each rule
	
	/**
	 * Temporary structures
	 */
	private Iterator<MapWritable> iterator; // Iterates over all hash maps
	private Iterator<Entry<Writable, Writable>> iteratorMap; // Iterates over a single hash map
	private Entry<Writable, Writable> matchingMap; // Current matching degrees hash map
	private ByteWritable[] classLabelsIndices; // Pre-stored class labels indices
	private float sumOthers, sumRemaining; // Sum of the matching degrees of other classes
	private float sum; // Sum of all matching degrees
	private byte classIndex; // Index of the class with the highest sum
	private byte currentClass; // Current class of the rule
	private float currentRW; // Current rule weight
	private float ruleWeight; // Computed rule weight
	private float[] classSum; // Sum of the matchings of each class
	private ArrayList<Byte> ruleClasses; // Classes of the current rule
	private byte[] generatedRule; // Rule antecedents and class index (at last position)
	
	/**
	 * Counters
	 */
	private int i, j; 
	
    @Override
    public void reduce(IntWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        
    	iterator = values.iterator();
    	
    	// Add all matching degrees
    	for (i = 0; i < classSum.length; i++)
    		classSum[i] = 0.0f;
    	sumRemaining = 0.0f;
    	ruleClasses = new ArrayList<Byte>();
    	while (iterator.hasNext()){
    		iteratorMap = iterator.next().entrySet().iterator();
    		while (iteratorMap.hasNext()){
    			matchingMap = iteratorMap.next();
    			try {
    				currentClass = ((ByteWritable)matchingMap.getKey()).get();
					classSum[currentClass] += ((FloatWritable)matchingMap.getValue()).get();
					ruleClasses.add(currentClass);
    			}
    			catch(Exception e){
    				sumRemaining += ((FloatWritable)matchingMap.getValue()).get();
    			}
    		}
    	}
    	
    	// Compute rule weight and solve conflicts
    	ruleWeight = 0.0f;
    	classIndex = -1;
    	sum = 0.0f;
    	for (i = 0; i < classSum.length; i++)
    		sum += classSum[i];
    	sum += sumRemaining;
		for (i = 0; i < classSum.length; i++){
			if (ruleClasses.contains((byte)i)){
				sumOthers = sumRemaining;
				for (j = 0; j < classSum.length; j++){
					if (j != i)
						sumOthers += classSum[j];
				}
				currentRW = (classSum[i] - sumOthers) / sum;
				if (currentRW > ruleWeight){
					ruleWeight = currentRW;
					classIndex = (byte)i;
				}
			}
		}
		
		if (ruleWeight > 0) {

	    	// Add class index to the rule
	    	generatedRule = new byte[ruleBase[key.get()].length+1];
	    	for (i = 0; i < generatedRule.length-1; i++)
	    		generatedRule[i] = ruleBase[key.get()][i];
	    	generatedRule[generatedRule.length-1] = classIndex;
	    	
	    	/*
	    	 * Key: Antecedents and class of the rule
	    	 * Value: Rule weight
	    	 */
	    	context.write(new ByteArrayWritable(generatedRule), new FloatWritable(ruleWeight));
    	
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
			System.err.println("\nSTAGE 3: ERROR READING CONFIGURATION\n");
			e.printStackTrace();
			System.exit(-1);
		}
		
		try {
			
			/**
			 * Initialize structures
			 */
			
			classLabelsIndices = new ByteWritable[Mediator.getNumClasses()];
	    	for (byte labelIndex = 0; labelIndex < classLabelsIndices.length; labelIndex++)
	    		classLabelsIndices[labelIndex] = new ByteWritable(labelIndex);
	    	
	    	classSum = new float[Mediator.getNumClasses()];
		    
		    /**
	    	 * Read the rule base
	    	 */
			
			ruleBase = new byte[Mediator.getLearnerRuleBaseSize()][Mediator.getNumVariables()];
	    	
	    	// Open the rule base
	    	Reader reader = new Reader(Mediator.getConfiguration(), 
	    			Reader.file(new Path(Mediator.getHDFSLocation()+Mediator.getLearnerRuleBaseTmpPath())));
	        ByteArrayWritable rule = new ByteArrayWritable();
	        ByteArrayWritable classLabels = new ByteArrayWritable();
	        
	        // Read the rule base
	        int id = 0;
	        while (reader.next(rule, classLabels)) {
	        	
	        	ruleBase[id] = rule.getBytes();
	        	id++;
	        	
	        }
	        reader.close();
		
		}
		catch(Exception e){
			System.err.println("\nSTAGE 3: ERROR READING RULE BASE\n");
			e.printStackTrace();
			System.exit(-1);
		}
	
	}
    
}

