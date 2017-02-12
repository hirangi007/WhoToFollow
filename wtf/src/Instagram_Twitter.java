 import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
  import java.util.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
  import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


 

  public class Instagram_Twitter {

   /*
    *As in pymk pdf step:1 - for each user F i followed by user X, the mapper emits F i as the key and X as the
value. 
emitted pairs will be (fi,x) and (x,-fi)
   */
    public static class IndexingMapper extends Mapper<Object, Text, Text, Text>{
 
       public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
          StringTokenizer st = new StringTokenizer(value.toString());
                   String x = st.nextToken();
                   
                   while (st.hasMoreTokens()) {
                       String fi=st.nextToken();
                       //now we have to keep track of the friends who are already in friendlist. So emit the pair of (x,-fi)
                       String friend="-";
                       String minus_friend=friend.concat(fi);
                       //emit (fi,x) and (x,-fi)
                       context.write(new Text(fi), new Text(x));
                       context.write(new Text(x), new Text(minus_friend));
               }
         }
     }

    
    /*
     * As shown in pdf algorithm,The reducer is the identity. It produces inverted lists of followers:
X, [ Y 1 , Y 2 , ... , Y k ]
where the Y i all follow user X.
So for user 1, all the friends pair will be emitted and combined in array
    */
    public static class IndexingReducer extends Reducer<Text, Text, Text, Text>{
    	@Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> persons_to_be_recommended=new ArrayList<String>();
            for(Text person: values){
                persons_to_be_recommended.add(person.toString());//for a particular key, values are added
            }
            //to convert ArraList to string
            String result="";
            for (String s : persons_to_be_recommended)
            {
                result += s + "\t";
            }
            context.write(key, new Text(result));
        }
}   

    // Main method
    public static void main(String[] args) throws Exception {
    	
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "JOB_INDEXING");
        job1.setJarByClass(Instagram_Twitter.class);
        job1.setMapperClass(IndexingMapper.class);
        job1.setReducerClass(IndexingReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        System.exit(job1.waitForCompletion(true) ? 0 : 1);
}

    
}
