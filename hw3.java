import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Hw3 {
    public static String param;
    public static class TotalMapper extends Mapper<Object, Text, Text, IntWritable>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = (value.toString()).split("\t");
            int sum = 0;
            if(!((line[2]).equals("duration_ms"))){
                sum = Integer.parseInt(line[2]);
            }
            final IntWritable one = new IntWritable(sum);
            context.write(new Text("total duration") ,one);

        }
    }
    public static class TotalReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private IntWritable sum = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int counter = 0;
            Text total = new Text("total duration");

            for (IntWritable val : values) {
                counter += val.get();
            }

            sum.set(counter);
            context.write(total, sum);
        }
    }

    public static class PopularityMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] line = (value.toString()).split("\t");

            context.write(new Text(line[0]), one);
        }
    }

    public static class PopularityReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int counter = 0;

            for (IntWritable val : values) {
                counter += val.get();
            }

            result.set(counter);
            context.write(key, result);
        }
    }

    public static class AverageMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private final static DoubleWritable one = new DoubleWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] line = (value.toString()).split("\t");
            int sum = 0;
            if(!line[2].equals("duration_ms")){
                sum = Integer.parseInt(line[2]);
                final DoubleWritable one = new DoubleWritable(sum);
                context.write(new Text("average duration") ,one);
            }
        }
    }

    public static class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            int counter = 0;
            double sum = 0.0;
            Text text = new Text("average duration");

            Iterator<DoubleWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                double value = iterator.next().get();
                sum += (value * 1.0);
                counter++;
            }

            result.set(sum/counter);
            context.write(text, result);
        }
    }



    public static class ExplicitMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        DoubleWritable one = new DoubleWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Text explicit = new Text();
            String[] line = (value.toString()).split("\t");
            if(line[3].equals("True")){
                one.set(Double.parseDouble(line[5].toString()));
                explicit.set("explicitly popular");
                context.write(explicit, one);
            }
            else if(line[3].equals("False"))
            {
                one.set(Double.parseDouble(line[5].toString()));
                explicit.set("not explicitly popular");
                context.write(explicit, one);
            }
        }
    }

    public static class ExplicitPartitioner extends Partitioner<Text,DoubleWritable> {
        public int getPartition(Text key, DoubleWritable value, int numReduceTasks){
            switch(key.toString()) {
                case "explicitly popular": return 0;
                case "not explicitly popular": return 1;
                default: return 0;
            }
        }
    }

    public static class ExplicitReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
        private DoubleWritable result = new DoubleWritable();
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            int counter = 0;
            double avg = 0.0;

            for (DoubleWritable val : values) {
                avg += val.get();
                counter++;
            }

            result.set(avg/counter);
            context.write(key, result);
        }

    }

    public static class DanceMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        DoubleWritable one = new DoubleWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Text dance = new Text();
            String[] line = (value.toString()).split("\t");
            if(!line[4].equals("year")){
                int year = Integer.parseInt(line[4]);
                one.set(Double.parseDouble(line[6].toString()));
                if(year <= 2002){
                    dance.set("before 2002");
                    context.write(dance, one);
                }
                else if(year > 2002 && year <=2012)
                {
                    dance.set("between 2002-2012");
                    context.write(dance, one);
                }
                else if(year > 2012){
                    dance.set("after 2012");
                    context.write(dance, one);
                }
            }
        }
    }

    public static class DancePartitioner extends Partitioner<Text,DoubleWritable> {
        public int getPartition(Text key, DoubleWritable value, int numReduceTasks){
            switch(key.toString()) {
                case "before 2002": return 0;
                case "between 2002-2012": return 1;
                case "after 2012": return 2;
                default: return 0;
            }
        }
    }

    public static class DanceReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
        private DoubleWritable result = new DoubleWritable();
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            int counter = 0;
            double sum = 0.0;

            for (DoubleWritable val : values) {
                sum += val.get();
                counter++;
            }

            result.set(sum/counter);
            context.write(key, result);
        }

    }

    public static void main(String[] args) throws Exception {
        param = args[0] ;
        if(param.equals("total")){
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "total");
            job.setJarByClass(Hw3.class);
            job.setMapperClass(TotalMapper.class);
            job.setCombinerClass(TotalReducer.class);
            job.setReducerClass(TotalReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        else if(param.equals("popular")){
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "popular");
            job.setJarByClass(Hw3.class);
            job.setMapperClass(PopularityMapper.class);
            job.setCombinerClass(PopularityReducer.class);
            job.setReducerClass(PopularityReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        else if(param.equals("average")){
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "average");
            job.setJarByClass(Hw3.class);
            job.setMapperClass(AverageMapper.class);
            job.setCombinerClass(AverageReducer.class);
            job.setReducerClass(AverageReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        else if(param.equals("explicitlypopular")){
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "explicitlypopular");
            job.setJarByClass(Hw3.class);
            job.setMapperClass(ExplicitMapper.class);
            job.setCombinerClass(ExplicitReducer.class);
            job.setReducerClass(ExplicitReducer.class);
            job.setNumReduceTasks(2);
            job.setOutputKeyClass(Text.class);
            job.setPartitionerClass(ExplicitPartitioner.class);
            job.setOutputValueClass(DoubleWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        else if(param.equals("dancebyyear")){
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "dancebyyear");
            job.setJarByClass(Hw3.class);
            job.setMapperClass(DanceMapper.class);
            job.setCombinerClass(DanceReducer.class);
            job.setReducerClass(DanceReducer.class);
            job.setNumReduceTasks(3);
            job.setOutputKeyClass(Text.class);
            job.setPartitionerClass(DancePartitioner.class);
            job.setOutputValueClass(DoubleWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }


    }

}