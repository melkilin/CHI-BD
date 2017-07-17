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

package es.unavarra.chi_bd.core;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.sun.org.apache.xml.internal.security.exceptions.Base64DecodingException;
import com.sun.org.apache.xml.internal.security.utils.Base64;

/**
 * Class containing all global objects and methods (this class is the only accessing configuration file)
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public class Mediator {
	
	/**
	 * Class labels field
	 */
	private static final String CLASS_LABELS_FIELD = "class_labels";
	
	/**
	 * Number of examples of each class field
	 */
	private static final String CLASS_NUM_EXAMPLES_FIELD = "class_num_examples";
	
	/**
	 * Classifier database path field
	 */
	private static final String CLASSIFIER_DATABASE_PATH_FIELD = "classifier_database_path";
	
	/**
	 * Classifier input path field
	 */
	private static final String CLASSIFIER_INPUT_PATH_FIELD = "classifier_input_path";
	
	/**
	 * Classifier output path field
	 */
	private static final String CLASSIFIER_OUTPUT_PATH_FIELD = "classifier_output_path";
	
	/**
	 * Classifier rule base path field
	 */
	private static final String CLASSIFIER_RULE_BASE_PATH_FIELD = "classifier_rule_base_path";
	
	/**
	 * Cost-sensitive field
	 */
	private static final String COST_SENSITIVE_FIELD = "cost-sensitive";
	
	/**
	 * Fuzzy Reasoning Method field
	 */
	private static final String FRM_FIELD = "inference";	
	
	/**
	 * HDFS location field
	 */
	private static final String HDFS_LOCATION_FIELD = "hdfs_location";
	
	/**
	 * Hadoop: number of mappers
	 */
	private static final String HADOOP_NUM_MAPPERS_FIELD = "hadoop_num_mappers";
	
	/**
	 * Hadoop: number of rules processed in the reduced
	 */
	private static final String hadoop_max_rules_reducer_FIELD = "hadoop_max_rules_reducer";
	
	/**
	 * Hadoop: maximum minutes with no status update in mapreduce tasks
	 */
	private static final String HADOOP_MAX_MINS_NO_UPDATE_FIELD = "hadoop_max_mins_no_update";
	
	/**
	 * Learner input path field
	 */
	private static final String LEARNER_INPUT_PATH_FIELD = "learner_input_path";
	
	/**
	 * Learner output path field
	 */
	private static final String LEARNER_OUTPUT_PATH_FIELD = "learner_output_path";
	
	/**
	 * Learner rule base (rule base that is learning) size field
	 */
	private static final String LEARNER_RULE_BASE_SIZE_FIELD = "learner_rule_base_size";
	
	/**
	 * Number of classes field
	 */
	private static final String NUM_CLASSES_FIELD = "num_classes";
	
	/**
	 * Number of the splits in the rules
	 */
	private static final String NUM_RULE_SPLITS_FIELD = "num_rule_splits";
	
	/**
	 * Number of linguistic labels field
	 */
	private static final String NUM_LINGUISTIC_LABELS_FIELD = "num_linguistic_labels";
	
	/**
	 * Number of variables field
	 */
	private static final String NUM_VARIABLES_FIELD = "num_variables";
	
	/**
	 * Minimum number of occurrences to consider a subset of antecedents as "frequent"
	 */
	private static final String MIN_FREQ_SUBSET_OCCURRENCE_FIELD = "min_freq_subset_occurrences";
	
	/**
	 * PATH FOR TIME STATS
	 */
	public static final String TIME_STATS_DIR = "time";
	
	/**
	 * Variables field
	 */
	private static final String VARIABLES_FIELD = "variables";
	
	/**
     * Class labels
     */
    private static String[] classLabels;
    
    /**
     * Most frequent class
     */
    private static byte classMostFrequent = 0;
    
    /**
     * Number of examples of each class
     */
    private static long[] classNumExamples;
    
    /**
     * Classifier database path
     */
    private static String classifierDatabasePath;
    
    /**
     * Classifier input directory path
     */
    private static String classifierInputPath;
    
    /**
     * Classifier output directory path
     */
    private static String classifierOutputPath;
    
    /**
     * Classifier rule base path
     */
    private static String classifierRuleBasePath;
    
    /**
     * Configuration object
     */
    private static Configuration configuration;
    
    /**
     * True for cost-sensitive rule weight computation (only for binary datasets)
     */
    private static boolean costSensitive;
    
    /**
     * Fuzzy Reasoning Method
     */
    private static byte frm;
    
    /**
     * Hadoop: number of mappers
     */
    private static int hadoopNumMappers;
    
    /**
     * Hadoop: number of rules processed in the reducer
     */
    private static int hadoopNumRulesReducer;
    
    /**
     * Hadoop: number of mappers
     */
    private static int hadoopMaxMinsNoUpdate;
	
	/**
     * HDFS Location
     */
    private static String hdfsLocation;
    
    /**
     * Learner input directory path
     */
    private static String learnerInputPath;
    
    /**
     * Learner output directory path
     */
    private static String learnerOutputPath;
    
    /**
     * Learner rule base (rule base that is learning) size
     */
    private static int learnerRuleBaseSize;
    
    /**
     * Number of class labels
     */
    private static byte numClassLabels = 0;
	
	/**
     * Number of linguistic labels (fuzzy sets) considered for all fuzzy variables
     */
    private static byte numLinguisticLabels = 0;
    
    /**
     * Number of frequent subsets of antecedents by rule
     */
    private static int numRuleSplits;
    
    /**
     * Number of variables
     */
    private static int numVariables = 0;
    
    /**
     * Minimum number of occurrences to consider a subset of antecedents as "frequent"
     */
    private static float minFreqSubsetOccurrence;
    
    /**
     * Start and end indices of each rule split
     */
    private static int[][] ruleSplitsIndices;
    
    /**
     * Variables of the problem
     */
    private static Variable[] variables;
    
    /**
     * Returns the needed split size to execute the specified number of mappers for the given input path
     * @param inputPath input path
     * @param numMappers number of mappers
     * @return needed split size to execute the specified number of mappers for the given input path
     * @throws IOException 
     */
    public static long computeHadoopSplitSize (String inputPath, int numMappers) throws IOException{
    	Path path = new Path(inputPath);
        FileSystem hdfs = path.getFileSystem(configuration);
        ContentSummary cSummary = hdfs.getContentSummary(path);
        return (cSummary.getLength() / numMappers);
    }
    
    /**
     * Computes the start and end indices of the rules splits
     */
    private static void computeRuleSplitsIndices (){
    	if (getNumVariables() < numRuleSplits)
    		numRuleSplits = getNumVariables();
    	ruleSplitsIndices = new int[numRuleSplits][2];
        int splitLength = getNumVariables() / numRuleSplits;
        int remaining = getNumVariables() % numRuleSplits;
        ruleSplitsIndices[0][0] = 0;
    	if (remaining > 0){
    		ruleSplitsIndices[0][1] = splitLength;
    		remaining--;
    	}
    	else
    		ruleSplitsIndices[0][1] = splitLength - 1;
        for (int numSplit = 1; numSplit < numRuleSplits; numSplit++){
        	ruleSplitsIndices[numSplit][0] = ruleSplitsIndices[numSplit-1][1] + 1;
        	if (remaining > 0){
        		ruleSplitsIndices[numSplit][1] = ruleSplitsIndices[numSplit][0] + splitLength;
        		remaining--;
        	}
        	else
        		ruleSplitsIndices[numSplit][1] = ruleSplitsIndices[numSplit][0] + splitLength - 1;
        }
    }
    
    /**
     * Returns classifier database path
     * @return classifier database path
     */
    public static String getClassifierDatabasePath (){
        return classifierDatabasePath;
    }
    
    /**
     * Returns the file path that stores the most frequent subsets of antecedents
     * @return file path that stores the most frequent subsets of antecedents
     */
    public static String getClassifierFrequentSubsetsPath (){
        return classifierRuleBasePath+".freq";
    }
	
	/**
     * Returns classifier input path
     * @return classifier input path
     */
    public static String getClassifierInputPath (){
        return classifierInputPath;
    }
    
    /**
     * Returns classifier output path
     * @return classifier output path
     */
    public static String getClassifierOutputPath (){
        return classifierOutputPath;
    }
    
    /**
     * Returns classifier rule base path
     * @return classifier rule base path
     */
    public static String getClassifierRuleBasePath (){
        return classifierRuleBasePath;
    }
    
    /**
     * Returns classifier temporary output path
     * @return classifier temporary output path
     */
    public static String getClassifierTmpOutputPath (){
        return classifierOutputPath+"_TMP";
    }
    
    /**
     * Returns class index
     * @param classLabel class label
     * @return class index
     */
    public static byte getClassIndex (String classLabel){
    	byte classIndex = -1;
        for (byte index = 0; index < classLabels.length; index++)
            if (classLabels[index].contentEquals(classLabel)){
                classIndex = index;
                break;
            }
        return classIndex;
    }
    
    /**
     * Returns class label
     * @param classIndex class index
     * @return class label
     */
    public static String getClassLabel(byte classIndex){
        return classLabels[classIndex];
    }
    
    /**
     * Returns class labels
     * @return class labels
     */
    public static String[] getClassLabels (){
        return classLabels;
    }
    
    /**
     * Returns the number of examples of each class
     * @return number of examples of each class
     */
    public static long[] getClassNumExamples (){
        return classNumExamples;
    }
    
    /**
     * Returns the number of examples of the class
     * @param classIndex index of the class
     * @return number of examples of the class
     */
    public static long getClassNumExamples (byte classIndex){
        return classNumExamples[classIndex];
    }
    
    /**
     * Returns the configuration object
     * @return Configuration object
     */
    public static Configuration getConfiguration (){
        return configuration;
    }
    
    /**
     * Returns the Fuzzy Reasoning Method used for the inference
     * @return 0 for Winning Rule and 1 for Additive Combination
     */
    public static byte getFRM (){
        return frm;
    }
    
    /**
     * Returns the number of Hadoop Mappers used for the execution
     * @return number of Hadoop Mappers used for the execution
     */
    public static int getHadoopNumMappers (){
        return hadoopNumMappers;
    }
    
    /**
     * Get the number of rules processed in the Hadoop Reducer
     * @return
     */
    public static int getHadoopNumRulesReducer (){
    	return hadoopNumRulesReducer;
    }
    
    /**
     * Returns the maximum minutes of MapReduce task execution with no status update
     * @return maximum minutes of MapReduce task execution with no status update
     */
    public static int getHadoopMaxMinsNoUpdate (){
        return hadoopMaxMinsNoUpdate;
    }
    
    /**
     * Returns the HDFS location
     * @return HDFS location
     */
    public static String getHDFSLocation (){
        return hdfsLocation+"/";
    }
    
    /**
     * Returns learner database path
     * @return learner database path
     */
    public static String getLearnerDatabasePath (){
    	return learnerOutputPath+"/DB";
    }
    
    /**
     * Returns the file path that stores the most frequent subsets of antecedents
     * @return file path that stores the most frequent subsets of antecedents
     */
    public static String getLearnerFrequentSubsetsPath (){
        return learnerOutputPath+"/RB.freq";
    }
    
    
    /**
     * Returns learner input path
     * @return learner input path
     */
    public static String getLearnerInputPath (){
        return learnerInputPath;
    }
    
    /**
     * Returns learner output path
     * @return learner output path
     */
    public static String getLearnerOutputPath (){
        return learnerOutputPath;
    }
    
    /**
     * Returns learner rule base path
     * @return learner rule base path
     */
    public static String getLearnerRuleBasePath (){
        return learnerOutputPath+"/RB";
    }
    
    /**
     * Returns learner rule base (rule base that is learning) size (number of rules)
     * @return learner rule base size (number of rules)
     */
    public static int getLearnerRuleBaseSize (){
        return learnerRuleBaseSize;
    }
    
    /**
     * Returns learner rule base temporary path
     * @return learner rule base temporary path
     */
    public static String getLearnerRuleBaseTmpPath (){
        return learnerOutputPath+"/RB.TMP";
    }
    
    /**
     * Returns learner stage 1 output path
     * @return learner stage 1 output path
     */
    public static String getLearnerStage1OutputPath (){
        return learnerOutputPath+"/_stage1";
    }
    
    /**
     * Returns learner stage 2 output path
     * @return learner stage 2 output path
     */
    public static String getLearnerStage2OutputPath (){
        return learnerOutputPath+"/_stage2";
    }
    
    /**
     * Returns learner stage 3 output path
     * @return learner stage 3 output path
     */
    public static String getLearnerStage3OutputPath (){
        return learnerOutputPath+"/_stage3";
    }
    
    /**
     * Returns the minimum number of occurrences to consider a subset of antecedents as "frequent"
     * @return minimum number of occurrences to consider a subset of antecedents as "frequent"
     */
    public static float getMinFreqSubsetOccurrence (){
    	return minFreqSubsetOccurrence;
    }
    
    /**
     * Returns the index of the most frequent class
     * @return index of the most frequent class
     */
    public static byte getMostFrequentClass (){
        return classMostFrequent;
    }
    
    /**
     * Returns the number of classes
     * @return number of classes
     */
    public static byte getNumClasses (){
    	if (classLabels == null)
    		return 0;
    	if (classLabels.length<128)
    		return (byte)classLabels.length;
    	else{
    		System.err.println("\nTHE NUMBER OF CLASS LABELS ("+classLabels.length+") EXCEEDS THE LIMIT (127)\n");
    		System.exit(-1);
    		return -1;
    	}
    }
    
    /**
     * Returns the number of linguistic labels (fuzzy sets) considered for all variables
     * @return number of linguistic labels (fuzzy sets) considered for all variables
     */
    public static byte getNumLinguisticLabels (){
        return numLinguisticLabels;
    }
    
    /**
     * Returns the number of splits in the rules
     * @return number of splits in the rules
     */
    public static int getNumRuleSplits (){
    	return numRuleSplits;
    }
    
    /**
     * Returns the number of variables
     * @return number of variables
     */
    public static int getNumVariables (){
    	if (variables != null)
    		return variables.length;
    	else
    		return 0;
    }
    
    /**
     * Returns the indices of each rule split (the first element is the split number, and the second element is [0] for the start index and [1] for the end index)
     * @return indices of each rule split (the first element is the split number, and the second element is [0] for the start index and [1] for the end index)
     */
    public static int[][] getRuleSplitsIndices (){
    	return ruleSplitsIndices;
    }
    
    /**
     * Returns all variables of the problem
     * @return all variables of the problem
     */
    public static Variable[] getVariables(){
        return variables;
    }
    
    /**
     * Reads classifier configuration (no variables and class labels are read here)
     * @throws Base64DecodingException 
     * @throws IOException 
     * @throws ClassNotFoundException 
     */
    public static void readClassifierConfiguration () throws Base64DecodingException, IOException, ClassNotFoundException{
    	
    	// Read basic configuration
    	classifierInputPath = configuration.get(CLASSIFIER_INPUT_PATH_FIELD);
    	classifierOutputPath = configuration.get(CLASSIFIER_OUTPUT_PATH_FIELD);
    	classifierDatabasePath = configuration.get(CLASSIFIER_DATABASE_PATH_FIELD);
    	classifierRuleBasePath = configuration.get(CLASSIFIER_RULE_BASE_PATH_FIELD);
    	hdfsLocation = configuration.get(HDFS_LOCATION_FIELD);
    	String frmStr = configuration.get(FRM_FIELD);
    	frm = (frmStr.contentEquals("wr")||
    			frmStr.contentEquals("winningrule")||frmStr.contentEquals("winning_rule"))?
    			RuleBase.FRM_WINNING_RULE:RuleBase.FRM_ADDITIVE_COMBINATION;
    	numLinguisticLabels = Byte.parseByte(configuration.get(NUM_LINGUISTIC_LABELS_FIELD));
    	numRuleSplits = Integer.parseInt(configuration.get(NUM_RULE_SPLITS_FIELD));
    	minFreqSubsetOccurrence = Float.parseFloat(configuration.get(MIN_FREQ_SUBSET_OCCURRENCE_FIELD));
    	
    	// Read the number of examples of each class
    	readClassNumExamples ();
    	
    	// Read data base
    	readDataBase();
    	
    	// Compute the start and end indices of each rule split
    	computeRuleSplitsIndices();
    		
    }
    
    /**
     * Reads the number of examples of each class
     * @param conf Configuration object
     * @throws Base64DecodingException 
     * @throws IOException 
     * @throws ClassNotFoundException 
     */
    private static void readClassNumExamples () throws Base64DecodingException, IOException, ClassNotFoundException {
    	
    	byte[] bytes = Base64.decode(configuration.get(CLASS_NUM_EXAMPLES_FIELD).getBytes());
    	ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
    	classNumExamples = (long[])objectInputStream.readObject();
    	
    	// Compute the most frequent class
    	byte i = 0;
    	long maxValue = -1;
    	for (i = 0; i < classNumExamples.length; i++){
    		if (classNumExamples[i] > maxValue){
    			maxValue = classNumExamples[i];
    			classMostFrequent = i;
    		}
    	}
    	
    }
    
    /**
     * Reads the number of examples of each class from the header file
     * @param filePath file path
	 * @throws IOException
     * @throws URISyntaxException 
     */
    public static void readClassNumExamplesFromHeaderFile (String filePath) throws IOException, URISyntaxException{
    	
    	Path pt=new Path(filePath);
        FileSystem fs = FileSystem.get(new java.net.URI(filePath),configuration);
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
    	String buffer = null;
        StringTokenizer st = null;
        
        ArrayList<Long> classNumExamples = new ArrayList<Long>();
        
        while ((buffer = br.readLine())!=null){
            
        	st = new StringTokenizer (buffer);
        	String field = st.nextToken();
        	
        	if (field.contentEquals("@numInstancesByClass")){
        		
        		st = new StringTokenizer (st.nextToken(),", ");
        		while (st.hasMoreTokens())
        			classNumExamples.add(Long.parseLong(st.nextToken()));
        		
        	}
        	
        }
        
        if (classNumExamples.isEmpty()){
			System.err.println("\nERROR READING HEADER FILE: The number of examples of each class is not specified\n");
			System.exit(-1);
		}
        
        // Save the number of examples of each class
        saveClassNumExamples(classNumExamples);
    	
    }
    
    /**
     * Reads variables and class labels from the data base
     * @throws IOException 
     * @throws IllegalArgumentException 
     * @throws ClassNotFoundException 
     */
    private static void readDataBase() throws IllegalArgumentException, IOException, ClassNotFoundException{
    	
    	// Read variables and class labels
    	FileSystem fs = FileSystem.get(configuration);
    	ObjectInputStream objectInputStream = new ObjectInputStream(fs.open(new Path(hdfsLocation+"/"+classifierDatabasePath)));
    	variables = (Variable[])objectInputStream.readObject();
    	classLabels = (String[])objectInputStream.readObject();
    	objectInputStream.close();
    	
    	// Save variables and class labels
    	saveVariables(variables);
    	saveClassLabels(classLabels);
    	
    }
    
    /**
     * Reads the configuration of Hadoop
     * @throws Base64DecodingException 
     * @throws IOException 
     * @throws ClassNotFoundException 
     */
    public static void readHadoopConfiguration () throws Base64DecodingException, IOException, ClassNotFoundException{
    	
    	hadoopNumMappers = Integer.parseInt(configuration.get(HADOOP_NUM_MAPPERS_FIELD));
    	hadoopNumRulesReducer = Integer.parseInt(configuration.get(hadoop_max_rules_reducer_FIELD));
    	hadoopMaxMinsNoUpdate = Integer.parseInt(configuration.get(HADOOP_MAX_MINS_NO_UPDATE_FIELD));
    		
    }
    
    /**
     * Reads header file and generates fuzzy variables
     * @param filePath file path
	 * @throws IOException
     * @throws URISyntaxException 
     */
    public static void readHeaderFile (String filePath) throws IOException, URISyntaxException{
    	
    	Path pt=new Path(filePath);
        FileSystem fs = FileSystem.get(new java.net.URI(filePath),configuration);
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
    	String buffer = null;
        StringTokenizer st = null;
        
        String output = "";
        ArrayList<Variable> variablesTmp = new ArrayList<Variable>();
        ArrayList<Long> classNumExamples = new ArrayList<Long>();
        
        byte num_linguistic_labels = Byte.parseByte(configuration.get(NUM_LINGUISTIC_LABELS_FIELD));
        
        while ((buffer = br.readLine())!=null){
            
        	buffer = buffer.replaceAll(", ", ",");
        	st = new StringTokenizer (buffer);
        	String field = st.nextToken();
        	
        	// Attribute
        	if (field.contentEquals("@attribute")){
        		
        		// Attribute name
        		String attribute = st.nextToken();
        		String name = null, type = null;
        		
        		// Check format
        		if (!attribute.contains("{") && !attribute.contains("[")) {
        			name = attribute;
	    			while (st.hasMoreTokens() && !attribute.contains("{") && !attribute.contains("[")){
	    				type = attribute;
	    				attribute = st.nextToken();
	    			}
	        		if (!attribute.contains("{") && !attribute.contains("[")) {
	        			System.err.println("\nERROR READING HEADER FILE: Values are not specified\n");
	        			System.exit(-1);
	        		}
        		}
        		else if (attribute.contains("[")) {
        			System.err.println("\nERROR READING HEADER FILE: Invalid attribute name\n");
        			System.exit(-1);
        		}
        		else {
        			name = attribute.substring(0,attribute.indexOf("{"));
        			type = name;
        		}
        		
	    		// Nominal attribute
	    		if (type == name && attribute.contains("{")){
	
					// Get nominal values
	    			attribute = attribute.substring(attribute.indexOf("{")+1);
					st = new StringTokenizer (attribute,"{}, ");
					String[] nominalValues = new String[st.countTokens()];
					byte counter = 0;
					while (st.hasMoreTokens()){
	        			nominalValues[counter] = st.nextToken();
	        			counter++;
	        		}
					
					// Build a new nominal variable
					NominalVariable newVariable = new NominalVariable(name);
					newVariable.setNominalValues(nominalValues);
					
					variablesTmp.add(newVariable);
	    		
	    		}
	    		// Numeric attribute
	    		else if (attribute.contains("[")){
	    			
	    			// Check format
	    			if (type != name && !type.toLowerCase().contentEquals("integer") 
    					&& !type.toLowerCase().contentEquals("real")) {
		    				System.err.println("\nERROR READING HEADER FILE: Invalid attribute type: '"+type+"'\n");
	        				System.exit(-1);
	    			}
	    			else if (type == name && !attribute.toLowerCase().contains("integer") 
    					&& !attribute.toLowerCase().contains("real")){
		    				System.err.println("\nERROR READING HEADER FILE: No attribute type is specified\n");
	        				System.exit(-1);
	    			}
	    			
	    			// Get upper and lower limits
	    			st = new StringTokenizer (attribute.substring(attribute.indexOf("[")+1),"[], ");
	        			
	    			double lowerLimit = Double.parseDouble(st.nextToken());
	    			double upperLimit = Double.parseDouble(st.nextToken());
	    			
	    			// Integer attribute
	    			if (attribute.toLowerCase().contains("integer")){
	    				
	    				// If the number of integer values is less than the number of
	    				// linguistic labels, then build a nominal variable
	    				if ((upperLimit - lowerLimit + 1) <= num_linguistic_labels){
	    					String[] nominalValues = new String[(int)upperLimit-(int)lowerLimit+1];
	    					for (int i = 0; i < nominalValues.length; i++)
	    						nominalValues[i] = Integer.valueOf(((int)lowerLimit+i)).toString();
	    					NominalVariable newVariable = new NominalVariable(name);
	    					newVariable.setNominalValues(nominalValues);
	    					variablesTmp.add(newVariable);
	    				}
	    				else {
	    	    			FuzzyVariable newVariable = new FuzzyVariable(name);
	    	    			newVariable.buildFuzzySets(lowerLimit,upperLimit,num_linguistic_labels);
	    	    			variablesTmp.add(newVariable);
	    				}
	    				
	    			}
	    			// Real attribute
	    			else {
    	    			FuzzyVariable newVariable = new FuzzyVariable(name);
    	    			newVariable.buildFuzzySets(lowerLimit,upperLimit,num_linguistic_labels);
    	    			variablesTmp.add(newVariable);
    				}
	    			
	    		}
	    		else {
	    			System.err.println("\nERROR READING HEADER FILE: Invalid format\n");
        			System.exit(-1);
	    		}
        		
        	}
        	else if (field.contentEquals("@outputs")){
        		
        		st = new StringTokenizer (st.nextToken(),", ");
        		if (st.countTokens()>1){
        			System.err.println("\nERROR READING HEADER FILE: This algorithm does not support multiple outputs\n");
        			System.exit(-1);
        		}
        		output = st.nextToken();
        		
        	}
        	else if (field.contentEquals("@numInstancesByClass")){
        		
        		st = new StringTokenizer (st.nextToken(),", ");
        		while (st.hasMoreTokens())
        			classNumExamples.add(Long.parseLong(st.nextToken()));
        		
        	}
        	
        }
        
        if (classNumExamples.isEmpty()){
			System.err.println("\nERROR READING HEADER FILE: The number of examples of each class is not specified\n");
			System.exit(-1);
		}
        
        // Remove output attribute from variable list and save it as the class
        Iterator<Variable> iterator = variablesTmp.iterator();
        while (iterator.hasNext()){
        	Variable variable = iterator.next();
        	if (output.contentEquals(variable.getName())){
        		// Save class labels
        		saveClassLabels(((NominalVariable)variable).getNominalValues());
        		// Remove from the list
        		iterator.remove();
        		break;
        	}
        }
        Variable[] newVariables = new Variable[variablesTmp.size()];
        for (int i = 0; i < variablesTmp.size(); i++)
        	newVariables[i] = variablesTmp.get(i);
        
        // Save variables
        saveVariables(newVariables);
        
        // Save the number of examples of each class
        saveClassNumExamples(classNumExamples);
        
        configuration.set(NUM_CLASSES_FIELD, Byte.toString(numClassLabels));
        configuration.set(NUM_VARIABLES_FIELD, Integer.toString(numVariables));
        configuration.set(LEARNER_RULE_BASE_SIZE_FIELD, "0");
    	
    }
    
    /**
     * Reads all the objects (variables, class labels, etc.) and parameters of this class (corresponding to the learning algorithm)
     * @throws Base64DecodingException 
     * @throws IOException 
     * @throws ClassNotFoundException 
     */
    public static void readLearnerConfiguration () throws Base64DecodingException, IOException, ClassNotFoundException{
    	
    	String buffer;
    	
    	buffer = configuration.get(COST_SENSITIVE_FIELD).toLowerCase().trim();
    	costSensitive = (buffer.contentEquals("true") || buffer.contentEquals("1"))? true:false;
    	
    	learnerInputPath = configuration.get(LEARNER_INPUT_PATH_FIELD);
    	learnerOutputPath = configuration.get(LEARNER_OUTPUT_PATH_FIELD);
    	learnerRuleBaseSize = Integer.parseInt(configuration.get(LEARNER_RULE_BASE_SIZE_FIELD));
    	hdfsLocation = configuration.get(HDFS_LOCATION_FIELD);
    	
    	numRuleSplits = Integer.parseInt(configuration.get(NUM_RULE_SPLITS_FIELD));
    	numLinguisticLabels = Byte.parseByte(configuration.get(NUM_LINGUISTIC_LABELS_FIELD));
    	numClassLabels = Byte.parseByte(configuration.get(NUM_CLASSES_FIELD));
    	numVariables = Integer.parseInt(configuration.get(NUM_VARIABLES_FIELD));
    	minFreqSubsetOccurrence = Float.parseFloat(configuration.get(MIN_FREQ_SUBSET_OCCURRENCE_FIELD));
    	
    	// Read class labels
    	byte[] bytes = Base64.decode(configuration.get(CLASS_LABELS_FIELD).getBytes());
    	ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
    	classLabels = (String[])objectInputStream.readObject();
    	
    	// Read variables
    	bytes = Base64.decode(configuration.get(VARIABLES_FIELD).getBytes());
    	objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
    	variables = (Variable[])objectInputStream.readObject();
    	
    	// Read the number of examples of each class
    	readClassNumExamples ();
    	
    	// Compute start and end indices of the rule splits
    	computeRuleSplitsIndices();
    		
    }
    
    /**
     * Stores classifier database path in the configuration file
     * @param newDatabasePath classifier database path
     */
    public static void saveClassifierDatabasePath (String newDatabasePath){
    	
    	configuration.set(CLASSIFIER_DATABASE_PATH_FIELD, newDatabasePath);
    	classifierDatabasePath = newDatabasePath;
    
    }
    
    /**
     * Stores classifier input path in the configuration file
     * @param newInputPath classifier input path
     */
    public static void saveClassifierInputPath (String newInputPath){
    	
    	configuration.set(CLASSIFIER_INPUT_PATH_FIELD, newInputPath);
    	classifierInputPath = newInputPath;
    
    }
    
    /**
     * Stores classifier output path in the configuration file
     * @param newOutputPath classifier output path
     */
    public static void saveClassifierOutputPath (String newOutputPath){
    	
    	configuration.set(CLASSIFIER_OUTPUT_PATH_FIELD, newOutputPath);
    	classifierOutputPath = newOutputPath;
    
    }
    
    /**
     * Stores classifier rule base path in the configuration file
     * @param newRuleBasePath classifier rule base path
     */
    public static void saveClassifierRuleBasePath (String newRuleBasePath){
    	
    	configuration.set(CLASSIFIER_RULE_BASE_PATH_FIELD, newRuleBasePath);
    	classifierRuleBasePath = newRuleBasePath;
    
    }
    
    /**
     * Stores class labels in the configuration file
     * @param newClassLabels class labels to be stored
     * @throws IOException 
     */
    private static void saveClassLabels (String[] newClassLabels) throws IOException {
    	
    	ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
	    ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
	    objectOutputStream.writeObject(newClassLabels);
	    objectOutputStream.close();
	    String classLabelsStr = new String(Base64.encode(byteArrayOutputStream.toByteArray()));
	    configuration.set(CLASS_LABELS_FIELD,classLabelsStr);
    	classLabels = newClassLabels;
    	numClassLabels = (byte)newClassLabels.length;
    
    }
    
    /**
     * Stores the number of examples of each class in the configuration file
     * @param numExamplesByClass number of examples of each class
     * @throws IOException 
     */
    private static void saveClassNumExamples (ArrayList<Long> numExamplesByClass) throws IOException {
    	
    	// Compute the most frequent class
    	long[] classNumExamplesArray = new long[numExamplesByClass.size()];
    	long maxValue = -1;
    	byte mostFrequentClass = 0;
    	byte i = 0;
    	for (Long element:numExamplesByClass) {
    		
    		classNumExamplesArray[i] = element.longValue();
    		if (classNumExamplesArray[i] > maxValue){
    			maxValue = classNumExamplesArray[i];
    			mostFrequentClass = i;
    		}
    		i++;
    	}
    	
    	// Convert it into a string
    	ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
	    ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
	    objectOutputStream.writeObject(classNumExamplesArray);
	    objectOutputStream.close();
	    String outputString = new String(Base64.encode(byteArrayOutputStream.toByteArray()));
	    
	    configuration.set(CLASS_NUM_EXAMPLES_FIELD,outputString);
    	classNumExamples = classNumExamplesArray;
    	classMostFrequent = mostFrequentClass;
    	
    }
    
    /**
     * Stores the HDFS location in the configuration file
     * @param newHDFSLocation HDFS location (hdfs://remote_address:remote_port)
     */
    public static void saveHDFSLocation (String newHDFSLocation){
    	
    	configuration.set(HDFS_LOCATION_FIELD, newHDFSLocation);
    	hdfsLocation = newHDFSLocation;
    
    }
    
    /**
     * Stores learner input path in the configuration file
     * @param newInputPath learner input path
     */
    public static void saveLearnerInputPath (String newInputPath){
    	
    	configuration.set(LEARNER_INPUT_PATH_FIELD, newInputPath);
    	learnerInputPath = newInputPath;
    
    }
    
    /**
     * Stores learner output path in the configuration file
     * @param newOutputPath learner output path
     */
    public static void saveLearnerOutputPath (String newOutputPath){
    	
    	configuration.set(LEARNER_OUTPUT_PATH_FIELD, newOutputPath);
    	learnerOutputPath = newOutputPath;
    
    }
    
    /**
     * Stores learner rule base size (number of rules) in the configuration file
     * @param size learner rule base size (number of rules)
     */
    public static void saveLearnerRuleBaseSize (int size){
    	
    	configuration.set(LEARNER_RULE_BASE_SIZE_FIELD, Integer.valueOf(size).toString());
    	learnerRuleBaseSize = size;
    
    }
    
    /**
     * Stores variables in the configuration file
     * @param newVariables variables to be added
     * @throws IOException 
     */
    private static void saveVariables (Variable[] newVariables) throws IOException {

    	ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
	    ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
	    objectOutputStream.writeObject(newVariables);
	    objectOutputStream.close();
	    String variablesStr = new String(Base64.encode(byteArrayOutputStream.toByteArray()));
	    configuration.set(VARIABLES_FIELD,variablesStr);
	    variables = newVariables;
    	numVariables = newVariables.length;
    	
    }
    
    /**
     * Sets a new configuration
     * @param newConfiguration configuration object
     */
    public static void setConfiguration (Configuration newConfiguration) {
    	configuration = newConfiguration;
    
    }
    
    /**
     * Stores the input configuration parameters in the configuration file
     * @param inputFilePath configuration file path
	 * @throws IOException 
     * @throws URISyntaxException 
     */
    public static void storeConfigurationParameters (String inputFilePath) throws IOException, URISyntaxException{
    	
    	Path pt = new Path(inputFilePath);
        FileSystem fs = FileSystem.get(new java.net.URI(inputFilePath),configuration);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        String buffer = null;
        StringTokenizer st = null;
        
        while ((buffer = br.readLine())!=null){
            st = new StringTokenizer (buffer, "= ");
            if (st.countTokens() == 2)
            	configuration.set(st.nextToken().toLowerCase(),st.nextToken().toLowerCase());
            else
            	throw new IOException();
        }
    	
    }
    
    /**
     * Returns whether the rule weight computation is cost-sensitive (only for binary datasets)
     * @return true if the rule weight computation is cost-sensitive
     */
    public static boolean useCostSensitive (){
    	return costSensitive;
    }

}
