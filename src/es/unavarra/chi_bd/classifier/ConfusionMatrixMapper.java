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

package es.unavarra.chi_bd.classifier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import es.unavarra.chi_bd.core.Mediator;
import es.unavarra.chi_bd.core.RuleBase;
import es.unavarra.chi_bd.utils.ByteArrayWritable;
import es.unavarra.chi_bd.utils.FrequentSubsetWritable;
import es.unavarra.chi_bd.utils.IntArrayWritable;

/**
 * Mapper class that classifies each example and returns a confusion matrix
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public class ConfusionMatrixMapper extends Mapper<Text, Text, ByteWritable, IntArrayWritable>{
    
	/**
	 * Rule base
	 */
	RuleBase ruleBase;

	/**
	 * Output
	 */
	IntArrayWritable[] confusionMatrix;
	
	/**
	 * Temporary structures
	 */
	String[] example;
	StringTokenizer st;
	int i;
	byte predictedClass, exampleClass;
	
	@Override
	protected void cleanup (Context context) throws IOException, InterruptedException{
		
		// Write the confusion matrix
		for (byte classIndex = 0; classIndex < Mediator.getNumClasses(); classIndex++)
			context.write(new ByteWritable(classIndex), confusionMatrix[classIndex]);
		
	}
	
	@Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		// Get the example and the class
        st = new StringTokenizer(key.toString()+value.toString()," ,");
        i = 0;
        while (st.countTokens() > 1){
        	example[i] = st.nextToken();
        	i++;
        }
        exampleClass = Mediator.getClassIndex(st.nextToken());
        
        // Classify the example
        predictedClass = ruleBase.classify(Mediator.getFRM(), example);
        confusionMatrix[exampleClass].getData()[predictedClass]++;

    }
	
	@Override
	protected void setup(Context context) throws InterruptedException, IOException{

		super.setup(context);
		
		// Read configuration
		try {
			Mediator.setConfiguration(context.getConfiguration());
			Mediator.readClassifierConfiguration();
		}
		catch(Exception e){
			System.err.println("\nHITS MAPPER: ERROR READING CONFIGURATION\n");
			e.printStackTrace();
		}
		
		// Intiate data structures
		confusionMatrix = new IntArrayWritable[Mediator.getNumClasses()];
		for (byte classIndex = 0; classIndex < Mediator.getNumClasses(); classIndex++)
			confusionMatrix[classIndex] = new IntArrayWritable(new int[Mediator.getNumClasses()]);
		example = new String[Mediator.getNumVariables()];
		
		// Read the rule base and frequent subsets
		try{
			
			/**
			 * READ THE RULE BASE
			 */
			
			// Open the file
	    	Reader reader = new Reader(Mediator.getConfiguration(), 
	    			Reader.file(new Path(Mediator.getHDFSLocation()+Mediator.getClassifierRuleBasePath())));
	    	ByteArrayWritable rule = new ByteArrayWritable();
	        FloatWritable ruleWeight = new FloatWritable();
	        
	        // Get rule splits indices and length
	        int[][] splitsIndex = Mediator.getRuleSplitsIndices();
	        int[] splitsLength = new int[Mediator.getNumRuleSplits()];
	        for (int numSplit = 0; numSplit < Mediator.getNumRuleSplits(); numSplit++)
	        	splitsLength[numSplit] = splitsIndex[numSplit][1]-splitsIndex[numSplit][0]+1;
	        
	        
	        // Read the rule base
	        ByteArrayWritable[] splitBytes;
	        ArrayList<ByteArrayWritable[]> splits = new ArrayList<ByteArrayWritable[]>();
	        ArrayList<Float> weights = new ArrayList<Float>();
	        ArrayList<Byte> classes = new ArrayList<Byte>();
	        int id = 0;
	        int j;
        	byte[] ruleSplit;
	        while (reader.next(rule, ruleWeight)) {
	        	
	        	// Store the splits of the rule
	        	splitBytes = new ByteArrayWritable[Mediator.getNumRuleSplits()];
	    		for (int numSplit = 0; numSplit < Mediator.getNumRuleSplits(); numSplit++){
	    		
	    			ruleSplit = new byte[splitsLength[numSplit]];
	    			j = 0;
	    	        for (i = splitsIndex[numSplit][0]; i <= splitsIndex[numSplit][1]; i++){
	    	        	ruleSplit[j] = rule.getBytes()[i];
	    	        	j++;
	    	        }
	    	        
	    	        splitBytes[numSplit] = new ByteArrayWritable(ruleSplit);
	    	        
	    		}
	    		
	    		// Add rule splits
	    		splits.add(splitBytes);
	    		
	    		// Store the rule weight
	    		weights.add(ruleWeight.get());
	    		
	    		// Store the class
	    		classes.add(rule.getBytes()[Mediator.getNumVariables()]);
	        	
	        	// Next rule
	        	id++;
	        	
	        }
	        reader.close();
	        
	        // Store the rule base
	        int numRules = id;
	        ByteArrayWritable[][] rulesSplits = new ByteArrayWritable[numRules][Mediator.getNumRuleSplits()];
	        float[] rulesWeights = new float[numRules];
	        byte[] rulesClasses = new byte[numRules];
	        for (id = 0; id < numRules; id++){
	        	rulesSplits[id] = splits.get(id);
	        	rulesWeights[id] = weights.get(id);
	        	rulesClasses[id] = classes.get(id);
	        }
	        splits.clear();
	        weights.clear();
	        classes.clear();
	        
	        /**
	         * READ THE FREQUENT SUBSETS
	         */
	    	
	    	// Open the file
	    	reader = new Reader(Mediator.getConfiguration(), 
	    			Reader.file(new Path(Mediator.getHDFSLocation()+Mediator.getClassifierFrequentSubsetsPath())));
	    	FrequentSubsetWritable subset = new FrequentSubsetWritable();
	        LongWritable occurrences = new LongWritable();
	        
	        // Read frequent antecedents
	        ArrayList<FrequentSubsetWritable>[] subsets = new ArrayList[Mediator.getNumRuleSplits()];
	        for (i = 0; i < subsets.length; i++) subsets[i] = new ArrayList<FrequentSubsetWritable>();
	        int numSubsets = 0;
	        while (reader.next(subset, occurrences)){
	        	subsets[subset.getNumSplit()].add(new FrequentSubsetWritable(subset));
	        	numSubsets++;
	        }
	        
	        // Store frequent antecedents
	        HashMap<ByteArrayWritable,Integer>[] freqSubsetsIndices = new HashMap[Mediator.getNumRuleSplits()];
	        i = 0;
	        for (int split = 0; split < Mediator.getNumRuleSplits(); split++){
	        	freqSubsetsIndices[split] = new HashMap((int)(subsets[split].size()/0.75) + 1);
	        	for (FrequentSubsetWritable element : subsets[split]){
	        		freqSubsetsIndices[split].put(
            			new ByteArrayWritable(element.getAntecedents()),Integer.valueOf(i));
	        		i++;
	        	}
	        }
	        
	        reader.close();
	        
	        // Indicate which subsets of each rule is a frequent subset
	        int[][] splitsIndices = new int[numRules][Mediator.getNumRuleSplits()];
	        Integer index;
	        for (i = 0; i < numRules; i++)
	        	for (j = 0; j < Mediator.getNumRuleSplits(); j++){
	        		index = freqSubsetsIndices[j].get(rulesSplits[i][j]);
	        		if (index != null)
	        			splitsIndices[i][j] = index;
	        		else
	        			splitsIndices[i][j] = -1;
	        	}
	        
	        /**
	         * CREATE THE RULE BASE
	         */
	        
	        ruleBase = new RuleBase(rulesSplits, rulesWeights, rulesClasses, splitsIndices, freqSubsetsIndices, numSubsets);
			
		}
		catch(Exception e){
			System.err.println("\nHITS MAPPER: ERROR READING RULE BASE\n");
			e.printStackTrace();
			System.exit(-1);
		}

	}
    
}
