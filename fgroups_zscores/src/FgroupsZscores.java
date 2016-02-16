
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apfloat.Apfloat;
import org.apfloat.ApfloatMath;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by lab on 13/02/16.
 */
public class FgroupsZscores {


    /*public static class SumCountTuple implements Writable {

        private double sum = 0;
        private long count = 0;

        public double getSum() {
            return sum;
        }

        public void setSum(double sum) {
            this.sum = sum;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        public void readFields(DataInput in) throws IOException {
            sum = in.readDouble();
            count = in.readLong();
        }

        public void write(DataOutput out) throws IOException {
            out.writeDouble(sum);
            out.writeLong(count);
        }

        public String toString() {
            return "" + sum + "\t" + count;
        }
    }*/

    public static class ZscoreProbEvalTuple implements Writable {
        private double sum = 0;
        private long count = 0;
        private double zscore = 0;
        private double prob = 0;
        private double eval = 0;

        public double getSum() {
            return sum;
        }

        public void setSum(double sum) {
            this.sum = sum;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        public double getZscore() {
            return zscore;
        }

        public void setZscore(double zscore) {
            this.zscore = zscore;
        }

        public double getProb() {
            return prob;
        }

        public void setProb(double prob) {
            this.prob = prob;
        }

        public double getEval() {
            return eval;
        }

        public void setEval(double eval) {
            this.eval = eval;
        }

        public void readFields(DataInput in) throws IOException {
            sum = in.readDouble();
            count = in.readLong();
            zscore = in.readLong();
            prob = in.readLong();
            eval = in.readLong();
        }

        public void write(DataOutput out) throws IOException {
            out.writeDouble(sum);
            out.writeLong(count);
            out.writeDouble(zscore);
            out.writeDouble(prob);
            out.writeDouble(eval);
        }

        public String toString() {
            return "" + sum + "\t" + count + "\t" + zscore + "\t" + prob + "\t" + eval;
        }
    }

    public static class SumCountMapper extends Mapper<Object, Text, Text, ZscoreProbEvalTuple> {
        private Text outGroupIds = new Text();
        private ZscoreProbEvalTuple outTuple = new ZscoreProbEvalTuple();

        public void map(Object key, Text value, Context context) throws
                IOException, InterruptedException, NumberFormatException, ArrayIndexOutOfBoundsException {
            String[] data = value.toString().split(" ");
            String f1 = data[3];
            String f2 = data[4];

            if (f1.compareTo(f2) >=0) {
                outGroupIds.set(f1+"\t"+f2);
            } else {
                outGroupIds.set(f2+"\t"+f1);
            }
            outTuple.setSum(Double.parseDouble(data[2]));
            outTuple.setCount(1);

            context.write(outGroupIds, outTuple);
        }
    }

    public static class SumCountMapperCombiner extends Reducer<Text, ZscoreProbEvalTuple, Text, ZscoreProbEvalTuple> {

        private ZscoreProbEvalTuple outTuple = new ZscoreProbEvalTuple();

        public void reduce(Text key, Iterable<ZscoreProbEvalTuple> values, Context context) throws
                IOException, InterruptedException {
            double sum = 0;
            long count = 0;

            for (ZscoreProbEvalTuple val: values) {
                sum += val.getSum();
                count += val.getCount();
            }
            outTuple.setSum(sum);
            outTuple.setCount(count);
            context.write(key, outTuple);
        }
    }

    public static class ApfloatZscoreProbEvalReducer extends Reducer<Text, ZscoreProbEvalTuple, Text, ZscoreProbEvalTuple> {
        private ZscoreProbEvalTuple outTuple = new ZscoreProbEvalTuple();
        private static final Apfloat sizeRatio = new Apfloat(0.3509);
        private static final Apfloat sizeIntercept = new Apfloat(-15.7498);
        private static final Apfloat sdRatio = new Apfloat(0.5901);
        private static final Apfloat sdIntercept = new Apfloat(2.0865);

        public void reduce(Text key, Iterable<ZscoreProbEvalTuple> values, Context context) throws
                IOException, InterruptedException {
            double sum = 0;
            long count = 0;

            for (ZscoreProbEvalTuple val: values) {
                sum += val.getSum();
                count += val.getCount();
            }
//            compute zscore, prob and evalue and set them
            if (count > 0) {
                Apfloat zscore = computeZscore(sum, count);
                Apfloat prob = computeProb(zscore);
                Apfloat eval = prob.multiply(new Apfloat(count));
                outTuple.setSum(sum);
                outTuple.setCount(count);
                outTuple.setZscore(zscore.doubleValue());
                outTuple.setProb(prob.doubleValue());
                outTuple.setEval(eval.doubleValue());
            }
            context.write(key, outTuple);
        }

        public static Apfloat computeZscore(double sum, long count) {
            Apfloat apsum = new Apfloat(sum);
            Apfloat apcount = new Apfloat(count);

            return apsum.subtract(
                    apcount.multiply(sizeRatio).add(sizeIntercept)
            ).divide(
                    ApfloatMath.exp(ApfloatMath.log(apcount).multiply(sdRatio).add(sdIntercept))
            );
//            return (sum-(0.3509*apcount-15.7498))/Math.exp(0.5901*Math.log(count)+2.0865);
        }

        public static Apfloat computeProb(Apfloat z) {

            Apfloat six = new Apfloat(6);
            Apfloat gamma = new Apfloat(0.577215665);
            Apfloat x = ApfloatMath.exp(
                    z.negate().multiply(ApfloatMath.pi(50)).divide(ApfloatMath.sqrt(six)).subtract(gamma)
            ).negate();

            if (z.intValue()>=28) {
                return x.add(ApfloatMath.pow(x, 2).divide(new Apfloat(2)))
                        .add(ApfloatMath.pow(x, 3).divide(new Apfloat(6))
                        ).negate();
            }
            return new Apfloat(1).subtract(ApfloatMath.exp(x));
        }
    }

    public static class ZscoreProbEvalReducer extends Reducer<Text, ZscoreProbEvalTuple, Text, ZscoreProbEvalTuple> {
        private ZscoreProbEvalTuple outTuple = new ZscoreProbEvalTuple();
        private static final double sizeRatio = new Double(0.3509);
        private static final double sizeIntercept = new Double(-15.7498);
        private static final double sdRatio = new Double(0.5901);
        private static final double sdIntercept = new Double(2.0865);

        public void reduce(Text key, Iterable<ZscoreProbEvalTuple> values, Context context) throws
                IOException, InterruptedException {
            double sum = 0;
            long count = 0;

            for (ZscoreProbEvalTuple val: values) {
                sum += val.getSum();
                count += val.getCount();
            }
//            compute zscore, prob and evalue and set them
            if (count > 0) {
                double zscore = computeZscore(sum, count);
                double prob = computeProb(zscore);
                double eval = prob*count;
                outTuple.setSum(sum);
                outTuple.setCount(count);
                outTuple.setZscore(zscore);
                outTuple.setProb(prob);
                outTuple.setEval(eval);
            }
            context.write(key, outTuple);
        }

        public static double computeZscore(double sum, long count) {
            return (sum-(0.3509*count-15.7498))/Math.exp(0.5901*Math.log(count)+2.0865);
        }

        public static double computeProb(double z) {

            double gamma = new Double(0.577215665);
            double sqrt_six = new Double(2.44948974278);
            double x = -1*Math.exp(
                    -1*z*Math.PI/sqrt_six - gamma
            );

            if (z>=28) {
                return -1*(x + Math.pow(x,2)/2 + Math.pow(x,3)/6);
            }
            return (1-Math.exp(x));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs =
                new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: CommentWordCount <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "StackOverflow Comment Word Count");
        job.setJarByClass(FgroupsZscores.class);
        job.setMapperClass(SumCountMapper.class);
        job.setCombinerClass(SumCountMapperCombiner.class);
        job.setReducerClass(ZscoreProbEvalReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ZscoreProbEvalTuple.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
