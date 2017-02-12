import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.util.*;
import java.util.function.Predicate;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
    /*
     * As in algorithm, for each inverted list X, [ Y 1 , Y 2 , ..., Y k ] the mapper emits all pairs (Y i ,Y j ) and
(Y j ,Y i ) where i ∈ [1, k], j ∈ [1, k] and i != j.
     */
    public static class AllPairsMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(values.toString());
            ArrayList<Integer> recommended_friend = new ArrayList<>();
            IntWritable user = new IntWritable(Integer.parseInt(st.nextToken()));            
            IntWritable friend1 = new IntWritable();
            while (st.hasMoreTokens()) {
              //all the positive values will be in recommended_friend and negative values will be in existing_friends
                Integer array_friend = Integer.parseInt(st.nextToken());
                if(array_friend < 0){
                	IntWritable existing_friend=new IntWritable();
                	existing_friend.set(array_friend);
                	context.write(user,existing_friend);
                }
                else                
                	recommended_friend.add(array_friend);                
            }
         // Now we can emit all (a,b) and (b,a) pairs
            // where a!=b and a & b are friends of user 'user'.
            // We use the same algorithm as in people you may know.
            ArrayList<Integer> seenFriends = new ArrayList<>();
            IntWritable friend2 = new IntWritable();
            for (Integer friend : recommended_friend) {
                friend1.set(friend);
                for (Integer seenFriend : seenFriends) {
                    friend2.set(seenFriend);
                    context.write(friend1, friend2);//emits all possible pair of friends
                    context.write(friend2, friend1);
                }
                seenFriends.add(friend1.get());
            }
        }
   }
   //RecommenderReducer is the same class as in people you may know
    public static class RecommenderReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

        private static class Recommendation {//includes functions used in the reducer

          
            private int friendId;
            private int nCommonFriends;

          
            public Recommendation(int friendId) {
                this.friendId = friendId;
                this.nCommonFriends = 1;
            }

       
            public int getFriendId() {
                return friendId;
            }

            public int getNCommonFriends() {
                return nCommonFriends;
            }

         
            public void addCommonFriend() {
                nCommonFriends++;
            }

             
            public String toString() {
                return friendId + "(" + nCommonFriends + ")";
            }
            public static Recommendation find(int friendId, ArrayList<Recommendation> recommendations) {
                for (Recommendation p : recommendations) {
                    if (p.getFriendId() == friendId) {
                        return p;
                    }
                }
                return null;
            }
        }
  
        /*
          THis reducer creates an array, counts the number of common friends, sorts and gives the people to follow after deleting  the ones
          which are aleady followed
        */ 
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            IntWritable user = key;
         // 'existingFriends' will store the friends of user 'user'
            // (the negative values in 'values').
            ArrayList<Integer> existingFriends = new ArrayList();
         // 'recommendedUsers' will store the list of user ids recommended
            // to user 'user'
            ArrayList<Integer> recommendedUsers = new ArrayList<>();
            while (values.iterator().hasNext()) {
                int value = values.iterator().next().get();
                if (value > 0) {
                    recommendedUsers.add(value);
                } else {
                    existingFriends.add(value);
                }
            }
         // 'recommendedUsers' now contains all the positive values in 'values'.
            // We need to remove from it every value -x where x is in existingFriends.
            // See javadoc on Predicate: https://docs.oracle.com/javase/8/docs/api/java/util/function/Predicate.html
            for (Integer friend : existingFriends) {
                recommendedUsers.removeIf(new Predicate<Integer>() {
                    @Override
                    public boolean test(Integer t) {
                        return t.intValue() == -friend.intValue();
                    }
                });
            }
             ArrayList<Recommendation> recommendations = new ArrayList<>();
          // Builds the recommendation array
             for (Integer userId : recommendedUsers) {
                Recommendation p = Recommendation.find(userId, recommendations);
                if (p == null) {
                    recommendations.add(new Recommendation(userId));
                } else {
                    p.addCommonFriend();
                }
            }
             
          // Sorts the recommendation array
             // See javadoc on Comparator at https://docs.oracle.com/javase/8/docs/api/java/util/Comparator.html
            recommendations.sort(new Comparator<Recommendation>() {
                @Override
                public int compare(Recommendation t, Recommendation t1) {
                    return -Integer.compare(t.getNCommonFriends(), t1.getNCommonFriends());
                }
            });
         // Builds the output string that will be emitted
            StringBuffer sb = new StringBuffer("");// Using a StringBuffer is more efficient than concatenating strings
            for (int i = 0; i < recommendations.size() && i < 10; i++) {
                Recommendation p = recommendations.get(i);
                sb.append(p.toString() + " ");
            }
            Text result = new Text(sb.toString());
            context.write(user, result);
        }
    }


    // Main method
    public static void main(String[] args) throws Exception {
        String indexing_output="generate.py-indexing-output";
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "JOB_INDEXING");
        job1.setJarByClass(Instagram_Twitter.class);
        job1.setMapperClass(IndexingMapper.class);
        job1.setReducerClass(IndexingReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(indexing_output));
        
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }
        
        Job job2 = Job.getInstance(conf, "JOB_2");
        job2.setJarByClass(Instagram_Twitter.class);
        job2.setJarByClass(Instagram_Twitter.class);
        job2.setMapperClass(AllPairsMapper.class);
        job2.setReducerClass(RecommenderReducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(indexing_output));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
}

    
}
