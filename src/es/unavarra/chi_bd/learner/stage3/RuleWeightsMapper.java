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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import es.unavarra.chi_bd.core.FuzzyRule;
import es.unavarra.chi_bd.core.FuzzyVariable;
import es.unavarra.chi_bd.core.Mediator;
import es.unavarra.chi_bd.utils.ByteArrayWritable;
import es.unavarra.chi_bd.utils.FrequentSubsetWritable;

/**
 * Mapper class that computes the matching of the rules with the examples of this mapper
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public class RuleWeightsMapper extends Mapper<Text, Text, IntWritable, MapWritable>{
    
	/**
	 * Rule base
	 */
	private ByteArrayWritable[][] rulesSplits; // Splits of all rules
	private ArrayList<Byte>[] rulesClasses; // Classes of each rule
	private float[] freqSubsetsMatching; // Stores the matching degrees of the most frequent subsets of antecedents for a given example
	private int[][] splitsIndices; // Indicates whether a split of a rule is a frequent subset or not, and its index (if it is frequent) in the matching table
	private float[][] aggMatchingDegrees; // Total matching degree of each class of each rule
	
	/**
	 * Temporary structures
	 */
	private MapWritable writableMatchingDegrees; // Total matching degrees stored in a writable Map
	private HashMap<ByteArrayWritable,Integer>[] freqSubsetsIndices; // Indicates the index of each frequent subset in the matching degrees table
	private Iterator<Entry<ByteArrayWritable, Integer>> freqSubsetsIterator; // Iterates over the hash map
	private Entry<ByteArrayWritable, Integer> freqSubsetEntry; // Hash Map entry
	private float[][] membershipDegrees; // Pre-computed membership degrees of a given example
	private int classLabelIndex; // Class index of a given example
	private StringTokenizer st; // String tokenizer for the input string
	private String[] inputValues; // Example input values
	
	/**
	 * Counters
	 */
	private int i;
	private byte label;
	private long startMs, endMs;
	
	@Override
	protected void cleanup (Context context) throws IOException, InterruptedException{
		
		float sum;
		byte j;
		
		// Write the matching degree of each class in each rule
		for (i = 0; i < Mediator.getLearnerRuleBaseSize(); i++){
			
			// Convert the array into MapWritable
			writableMatchingDegrees = new MapWritable();
			
			// Write the matching degrees of each class of the rule
			for (j = 0; j < rulesClasses[i].size(); j++){
				writableMatchingDegrees.put(new ByteWritable(rulesClasses[i].get(j)), 
					new FloatWritable(aggMatchingDegrees[i][rulesClasses[i].get(j)]));
			}
			
			// Default key (matching degrees of the remaining classes)
			sum = 0;
			for (j = 0; j < Mediator.getNumClasses(); j++){
				if (!rulesClasses[i].contains(j))
					sum += aggMatchingDegrees[i][j];
			}
			writableMatchingDegrees.put(NullWritable.get(), new FloatWritable(sum));
			
			/*
	    	 * Key: Rule ID
	    	 * Value: Matching degrees of the classes (of the rule i)
	    	 */
			context.write(new IntWritable(i), writableMatchingDegrees);
			
		}
		
		// Write execution time
		endMs = System.currentTimeMillis();
		long mapperID = context.getTaskAttemptID().getTaskID().getId();
		try {
        	FileSystem fs = FileSystem.get(Mediator.getConfiguration());
        	Path file = new Path(Mediator.getHDFSLocation()+"/"+Mediator.getLearnerOutputPath()+"/"+Mediator.TIME_STATS_DIR+"/stage3_mapper"+mapperID+".txt");
        	OutputStream os = fs.create(file);
        	BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os));
        	bw.write("Execution time (seconds): "+((endMs-startMs)/1000));
        	bw.close();
        	os.close();
        }
        catch(Exception e){
        	System.err.println("\nSTAGE 3: ERROR WRITING EXECUTION TIME");
			e.printStackTrace();
        }
		
	}
	
	@Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		/**
		 * Read input values and the class.
		 * Pre-compute membership degrees.
		 */

        st = new StringTokenizer(key.toString()+value.toString(), ", ");
        i = 0; // Variable counter
        
        // Read example
        while (st.hasMoreTokens()){
        	
        	// Read input values
        	if (st.countTokens() > 1){
        		inputValues[i] = st.nextToken();
        		// Compute the membership degree of the current value to all linguistic labels
        		if (Mediator.getVariables()[i] instanceof FuzzyVariable)
        			for (label = 0; label < Mediator.getNumLinguisticLabels(); label++)
        				membershipDegrees[i][label] = FuzzyRule.computeMembershipDegree(i,label,inputValues[i]);
        	}
        	// Read the class of the example
        	else{
        		// Get class index
        		classLabelIndex = Mediator.getClassIndex(st.nextToken());
        	}
        	
        	// Increase variable counter
        	i++;
        	
        }
        
        /**
         * Pre-compute the partial matching degree of the example with all frequent subsets of antecedents
         */
        
        for (i = 0; i < Mediator.getNumRuleSplits(); i++){
        	freqSubsetsIterator = freqSubsetsIndices[i].entrySet().iterator();
	        while (freqSubsetsIterator.hasNext()){
	        	freqSubsetEntry = freqSubsetsIterator.next();
	        	freqSubsetsMatching[freqSubsetEntry.getValue()] = 
        			FuzzyRule.computePartialMatchingDegree(
					membershipDegrees, i, freqSubsetEntry.getKey().getBytes(), inputValues);
	        }
        }
        
		/**
		 * Compute the matching degree of the example with all rules
		 */
        
        // Compute the matching degree with all the rules
		for (i = 0; i < Mediator.getLearnerRuleBaseSize(); i++)
			aggMatchingDegrees[i][classLabelIndex] += FuzzyRule.computeMatchingDegree(
				membershipDegrees, freqSubsetsMatching, splitsIndices[i], rulesSplits[i], inputValues);
		
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
		
		startMs = System.currentTimeMillis();
		
		try {
			
			/**
			 * Initialize structures
			 */
			
			inputValues = new String[Mediator.getNumVariables()];
			membershipDegrees = new float[Mediator.getNumVariables()][Mediator.getNumLinguisticLabels()]; // Pre-computed membership degrees
			FuzzyRule.setNumSplits();
	    	
	    	aggMatchingDegrees = new float[Mediator.getLearnerRuleBaseSize()][Mediator.getNumClasses()];
	    	for (i = 0; i < Mediator.getLearnerRuleBaseSize(); i++)
	    		for (int j = 0; j < Mediator.getNumClasses(); j++)
	    			aggMatchingDegrees[i][j] = 0.0f;
		    
		    /**
	    	 * Read the rule base
	    	 */
	    	
	    	rulesSplits = new ByteArrayWritable[Mediator.getLearnerRuleBaseSize()][Mediator.getNumRuleSplits()];
	    	rulesClasses = new ArrayList[Mediator.getLearnerRuleBaseSize()];
	    	
	    	// Open the rule base
	    	Reader reader = new Reader(Mediator.getConfiguration(), 
	    			Reader.file(new Path(Mediator.getHDFSLocation()+Mediator.getLearnerRuleBaseTmpPath())));
	        ByteArrayWritable rule = new ByteArrayWritable();
	        ByteArrayWritable classLabels = new ByteArrayWritable();
	        
	        // Get rule splits indices and length
	        int[][] splitsIndex = Mediator.getRuleSplitsIndices();
	        int[] splitsLength = new int[Mediator.getNumRuleSplits()];
	        for (int numSplit = 0; numSplit < Mediator.getNumRuleSplits(); numSplit++)
	        	splitsLength[numSplit] = splitsIndex[numSplit][1]-splitsIndex[numSplit][0]+1;
	        
	        // Read the rule base
	        int id = 0;
	        int j;
        	byte[] ruleSplit;
	        while (reader.next(rule, classLabels)) {
	        	
	        	// Store the splits of the rule
	    		for (int numSplit = 0; numSplit < Mediator.getNumRuleSplits(); numSplit++){
	    		
	    			ruleSplit = new byte[splitsLength[numSplit]];
	    			j = 0;
	    	        for (i = splitsIndex[numSplit][0]; i <= splitsIndex[numSplit][1]; i++){
	    	        	ruleSplit[j] = rule.getBytes()[i];
	    	        	j++;
	    	        }
	    	        
	    	        rulesSplits[id][numSplit] = new ByteArrayWritable(ruleSplit);
	    	        
	    		}
	    		
	    		// Store the class labels indices
	    		rulesClasses[id] = new ArrayList<Byte>();
	    		for (i = 0; i < classLabels.getBytes().length; i++)
	    			rulesClasses[id].add(classLabels.getBytes()[i]);
	        	
	        	// Next rule
	        	id++;
	        	
	        }
	        reader.close();
	        
	        /**
	         * Read frequent subsets
	         */
	    	
	    	// Open the file
	    	reader = new Reader(Mediator.getConfiguration(), 
	    			Reader.file(new Path(Mediator.getHDFSLocation()+Mediator.getLearnerFrequentSubsetsPath())));
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
	        freqSubsetsIndices = new HashMap[Mediator.getNumRuleSplits()];
	        freqSubsetsMatching = new float[numSubsets];
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
	        
	        /**
	         * Indicate which subsets of each rule is a frequent subset
	         */
	        
	        splitsIndices = new int[Mediator.getLearnerRuleBaseSize()][Mediator.getNumRuleSplits()];
	        Integer index;
	        for (i = 0; i < Mediator.getLearnerRuleBaseSize(); i++)
	        	for (j = 0; j < Mediator.getNumRuleSplits(); j++){
	        		index = freqSubsetsIndices[j].get(rulesSplits[i][j]);
	        		if (index != null)
	        			splitsIndices[i][j] = index;
	        		else
	        			splitsIndices[i][j] = -1;
	        	}
		
		}
		catch(Exception e){
			System.err.println("\nSTAGE 3: ERROR READING THE RULE BASE\n");
			e.printStackTrace();
			System.exit(-1);
		}
	
	}
	
}

