import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections4.IterableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private final Text word = new Text();
        private final Text pairwWrite = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //System.out.println("Mapper path: "+context.getInputSplit().toString());
            String[] demo = (context.getInputSplit().toString().split("\\+")[0]).split("/");
            String docID=demo[demo.length-1];
            StringTokenizer itr = new StringTokenizer(value.toString());
            //HashMap<String,Integer> map=new HashMap<>();
            while (itr.hasMoreTokens()) {
                String w=itr.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase();;
                if(w.length()!=0) {
                    word.set(w);
                    pairwWrite.set(docID);
                    //outputPair.set(pair);
                    context.write(word, pairwWrite);
                }
            }

        }
    }

    public static class InvIdxReducer
            extends Reducer<Text, Text, Text, Text> {
        //private IntWritable result = new IntWritable();
        //private ObjectWritable result = new ObjectWritable();
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
           // System.out.println("Reducer start");
            double N = 13806;
            //ArrayList<String> invertedIdx = new ArrayList<>();
            String invertedIdx="";
            HashMap<String,Integer> map=new HashMap<>();

            for (Text val : values) {
                if(map.containsKey(val.toString())){
                    map.put(val.toString(),map.get(val.toString())+1);
                }

                else map.put((val.toString()),1);
                /*
                String[] s = val.toString().split(",");
              //  System.out.println("Reducer loop: "+s[1]);
                Double tf=Double.parseDouble(s[1]);

                double idf = N/IterableUtils.size(values);
                //Pair pair = new Pair(p.key,tf*idf);
                String fin = s[0]+":"+ (tf*idf);
                invertedIdx+=fin+",";*/

            }

            double idf = N/map.size();
            String finalList="";
            for(String title: map.keySet()){
                finalList+=title+"#"+(map.get(title)*idf)+",";
            }

            context.write(key, new Text(finalList));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(InvIdxReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
