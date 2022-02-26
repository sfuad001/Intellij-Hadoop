import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import javafx.util.Pair;
import org.apache.commons.collections4.IterableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, ObjectWritable> {

        private final static ObjectWritable outputPair = new ObjectWritable();
        private final Text word = new Text();


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            HashMap<String,Integer> map=new HashMap<>();
            while (itr.hasMoreTokens()) {
                String w=itr.nextToken();
                if(map.containsKey(w))
                    map.replace(w,map.get(w)+1);
                else
                    map.put(w,1);

                //word.set(itr.nextToken());
                //context.write(word, one);
            }
            String[] parts = value.toString().split("\n");
            for(String k:map.keySet()){
                Pair<String, Integer> pair = new Pair<>(parts[0],map.get(k));
                word.set(k);
                outputPair.set(pair);
                context.write(word, outputPair);
            }
        }
    }

    public static class InvIdxReducer
            extends Reducer<Text, ObjectWritable, Text, ObjectWritable> {
        //private IntWritable result = new IntWritable();
        private ObjectWritable result = new ObjectWritable();
        public void reduce(Text key, Iterable<ObjectWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            double N = 13806;
            ArrayList<Pair> invertedIdx = new ArrayList<>();
            for (ObjectWritable val : values) {
                 Pair p= (Pair)(val.get());
                 Integer tf=(Integer)(p.getValue());
                 double idf = IterableUtils.size(values)/N;
                 Pair<String, Double> pair = new Pair<>((String)p.getKey(),tf*idf);
                 invertedIdx.add(pair);
            }
            result.set(invertedIdx);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(InvIdxReducer.class);
        job.setReducerClass(InvIdxReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ObjectWritable.class);
        FileInputFormat.addInputPath(job, new Path("./lucenedemo/"));
        FileOutputFormat.setOutputPath(job, new Path("./output3/"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}