 import java.io.IOException;


  import org.apache.hadoop.io.Text;
 
  import java.util.*;
  import org.apache.hadoop.mapreduce.Mapper;
  import org.apache.hadoop.mapreduce.Reducer;

 

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
       
    	
}
   /*
    This mapper ignores the key and maps every value to every other value and emits if the value is non-negative
   */
   

    // Main method
    public static void main(String[] args) throws Exception {
     

      
}

    
}
