package project3;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class Project3 {
	//CREATE MAPPER CLASS

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1); 
    private Text keyhere = new Text(); //CREATED TEXT FOR KEY

    
    // CREATE MAP FUNCTION
    public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException 
    {	
		FileSplit fileSplit = (FileSplit)context.getInputSplit(); //WE DID FILESPLIT OF INPUT FILE
		String filename = fileSplit.getPath().getName(); //GOT FILENAME
		int count = 1; 
		String[] itr = (value.toString().split(";")); //SPLIT DATA BY ; TO GET ATTRIBUTES
		if(!itr[3].equals("\\N") && itr.length <= 5)  // SELECTED ONLY VALID FIELDS THAT IS ATTRIBUTES LESS THAN 5 AND YEAR IS VALID
		{
			if(itr[1].equals("movie")) // SELECTED DATA WITH TYPE MOVIE ONLY
			{
				if(Integer.parseInt(itr[3]) >= 2001 && Integer.parseInt(itr[3]) <= 2005) // SELECTED DATA OF YEAR FROM 2001 TO 2005
				{
					
					String[] itr2 = (itr[4].toString().split(",")); // SPLIT MOVIE GENRE BY ,
					//System.out.println("Next list");
					List<String> list = Arrays.asList(itr2); // CONVERTED TO LIST SO COULD BE EASIY CHECKED
					if(list.contains("Comedy") && list.contains("Romance")) // CHECK WHETHER THE GENRE IS COMEDY AND ROMANCE
					{
						keyhere.set("[2001-2005];Comedy,Romance;"); // SET KEY AS THIS
		    			context.write(keyhere, one); // VALUE AS  1 FOR THE ABOVE SET KEY
					}
					else if(list.contains("Action") && list.contains("Thriller")) // CHECK WHETHER THE GENRE IS ACTION AND THRILLER
					{
						keyhere.set("[2001-2005];Action,Thriller;"); // SET KEY AS THIS
		    			context.write(keyhere, one); // VALUE AS 1 FOR THE ABOVE KEY
					}
					else if(list.contains("Adventure") && list.contains("Sci-Fi")) // CHECK WHETHER THE GENRE IS ADVENTURE AND SCI-FI
					{
						keyhere.set("[2001-2005];Adventure,Sci-Fi;"); // SET KEY AS THIS
		    			context.write(keyhere, one); // VALUE AS 1 FOR THE ABOVE KEY
					}
		
					
				}
				else if(Integer.parseInt(itr[3]) >= 2006 && Integer.parseInt(itr[3]) <= 2010) // SELECTED DATA OF YEAR FROM 2006 TO 2010
				{
					
					String[] itr2 = (itr[4].toString().split(",")); // SPLIT MOVIE GENRE BY ,
					//System.out.println("Next list");
					List<String> list = Arrays.asList(itr2);// CONVERTED TO LIST SO COULD BE EASILY CHECKED
					if(list.contains("Comedy") && list.contains("Romance")) // CHECK WHETHER THE GENRE IS COMEDY AND ROMANCE
					{
						keyhere.set("[2006-2010];Comedy,Romance;"); // SET KEY AS THIS
		    			context.write(keyhere, one); // VALUE AS 1 FOR THE ABOVE KEY
					}
					else if(list.contains("Action") && list.contains("Thriller")) // CHECK WHETHER THE GENRE IS ACTION AND THRILLER
					{
						keyhere.set("[2006-2010];Action,Thriller;"); // SET KEY AS THIS
		    			context.write(keyhere, one); // VALUE AS 1 FOR THE ABOVE KEY
					}
					else if(list.contains("Adventure") && list.contains("Sci-Fi")) // CHECK WHETHER THE GENRE IS ADVENTURE AND SCI-FI
					{
						keyhere.set("[2006-2010];Adventure,Sci-Fi;"); // SET KEY AS THIS
		    			context.write(keyhere, one); // VALUE AS 1 FOR THE ABOVE KEY
					} 
					
				}
				else if(Integer.parseInt(itr[3]) >= 2011 && Integer.parseInt(itr[3]) <= 2015) // SELECTED DATA OF YEAR FROM 2011 TO 2015
				{
					
					String[] itr2 = (itr[4].toString().split(",")); // SPLIT MOVIE GENRE BY ,
					//System.out.println("Next list");
					List<String> list = Arrays.asList(itr2); // CONVERTED TO LIST SO COULD BE EASILY CHECKED
					if(list.contains("Comedy") && list.contains("Romance")) // CHECK WHETHER THE GENRE IS COMEDY AND ROMANCE
					{
						keyhere.set("[2011-2015];Comedy,Romance;"); // SET KEY AS THIS
		    			context.write(keyhere, one); // VALUE AS 1 FOR THE ABOVE KEY
					}
					else if(list.contains("Action") && list.contains("Thriller")) // CHECK WHETHER THE GENRE IS ACTION AND THRILLER
					{
						keyhere.set("[2011-2015];Action,Thriller;"); // SET KEY AS THIS
		    			context.write(keyhere, one); // VALUE AS 1 FOR THE ABOVE KEY
					}
					else if(list.contains("Adventure") && list.contains("Sci-Fi")) // CHECK WHETHER THE GENRE IS ADVENTURE AND SCI-FI
					{
						keyhere.set("[2011-2015];Adventure,Sci-Fi;"); // SET KEY AS THIS
		    			context.write(keyhere, one); // VALUE AS 1 FOR THE ABOVE KEY
					}
			
					
				}
			}
		}
    }
  }  
    
  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable(); //CRAETE RESULT FOR FINAL VALUE
    // CREATED REDUCE FUNCTION
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      // RUN AN ITERATOR OVER INTERMEDIATE VALUE GENERTAED BY MAPPER
      for (IntWritable val : values) {
        sum += val.get(); // SUMATION OF VALUE FROR RESPECTIVE KEYS
      }
      result.set(sum); // SET ADDED FILE
      context.write(key, result); //FLUSH DATA ON OUTPUT FILE
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count"); // JOB CREATED
    job.setJarByClass(Project3.class); // SET JAT BY CLASS
    job.setMapperClass(TokenizerMapper.class); // SET MAPPER CLASS
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class); //SET REDUCER CLASS
    job.setOutputKeyClass(Text.class); // SET OUTPUT TEXT
    job.setOutputValueClass(IntWritable.class); // VALUES INT WRITABLE
    FileInputFormat.addInputPath(job, new Path(args[0])); // SET INPUT FILE AS FIRST ARGUMENT
    FileOutputFormat.setOutputPath(job, new Path(args[1])); //SET OUTPUT FILE AS SECOND ARGUMENT
    System.exit(job.waitForCompletion(true) ? 0 : 1); // JOB WAIT UNTIL COMPLETION
  }
}
