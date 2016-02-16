import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;
import java.util.*;

/**
 * Created by lab on 15/02/16.
 */
public class SampleBgDistribution {

    public static class SampleDistTuple implements Writable {
        private int count = 0;
        private double probSum = 0;
        private double scoreSum = 0;

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public double getProbSum() {
            return probSum;
        }

        public void setProbSum(double probSum) {
            this.probSum = probSum;
        }

        public double getScoreSum() {
            return scoreSum;
        }

        public void setScoreSum(double scoreSum) {
            this.scoreSum = scoreSum;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(count);
            dataOutput.writeDouble(probSum);
            dataOutput.writeDouble(scoreSum);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            count = dataInput.readInt();
            probSum = dataInput.readDouble();
            scoreSum = dataInput.readDouble();
        }

        public String toString() {
            return "" + count + " " + probSum + " " + scoreSum;
        }
    }

    public static class SampleDistMapper extends Mapper<Object, Text, Text, SampleDistTuple> {

        private Logger logger = Logger.getLogger(SampleDistMapper.class);

        private Text probSampleNo = new Text();
        private SampleDistTuple sample = new SampleDistTuple();
        private ArrayList<ProbScoreArrayList.ProbScoreTuple> probScoreList;
        private static Random rand;
        private static String PROBSCORE_FILENAME = "probScoreList.json";

        /*public static <T> Set<T> randomSample(List<T> items, int m){
            HashSet<T> res = new HashSet<T>();
            int n = items.size();
            if (m > n/2){ // The optimization
                Set<T> negativeSet = randomSample(items, n - m);
                for(T item : items){
                    if (!negativeSet.contains(item))
                        res.add(item);
                }
            }else{ // The main loop
                while(res.size() < m){
                    int randPos = rand.nextInt(n);
                    res.add(items.get(randPos));
                }
            }
            return res;
        }*/

        public static <T> void randomSample(List<T> items, int m){
            for(int i=0;i<m;i++){
                int pos = i + rand.nextInt(items.size() - i);
                T tmp = items.get(pos);
                items.set(pos, items.get(i));
                items.set(i, tmp);
            }
        }


        @Override
        protected void setup(Mapper<Object, Text, Text, SampleDistTuple>.Context context)
                throws IOException, InterruptedException {
            BasicConfigurator.configure();
            logger.info("Entering setup.");

            if (context.getCacheFiles() != null
                    && context.getCacheFiles().length > 0) {

                logger.info("Opening serialized file.");
                logger.info("URI value is: " + context.getCacheFiles()[0].toString());
                FileSystem fileSystem = FileSystem.get(context.getConfiguration());
//                File probScoreListFile = new File(PROBSCORE_FILENAME);
//                File probScoreListFile = new File(context.getCacheFiles()[0]);
                Path probScoreListFile = new Path(context.getCacheFiles()[0]);
//                try {

//                    FileInputStream fileIn = new FileInputStream(probScoreListFile);
                    FSDataInputStream fileIn = fileSystem.open(probScoreListFile);
                    Reader in = new BufferedReader(new InputStreamReader(fileIn));
                    if (in.ready()) {
                        logger.info("Opened serialized file.");
                        logger.info("Ready to read serialized file.");
                    } else {
                        logger.info("NOT!!! Ready to read serialized file.");
                    }
                    Gson gson = new Gson();
                    probScoreList = gson.fromJson(in, new TypeToken<ArrayList<ProbScoreArrayList.ProbScoreTuple>>() {}.getType());

                    logger.info("Done reading serialized file.");

                    in.close();
                    fileIn.close();

                    logger.info("Closing serialized file.");
                //} catch ()
                /*try {

                    logger.info("Reading serialized file.");

                    FileInputStream fileIn = new FileInputStream(probScoreListFile);
                    ObjectInputStream in = new ObjectInputStream(fileIn);
                    probScoreList = (ArrayList<ProbScoreArrayList.ProbScoreTuple>) in.readObject();

                    logger.info("Done reading serialized file.");

                    in.close();
                    fileIn.close();

                    logger.info("Closing serialized file.");

                } catch (ClassNotFoundException e) {
                    IOException out = new IOException(e.getMessage(), e.getCause());
                    out.setStackTrace(e.getStackTrace());
                    throw out;
                }*/
            }

            super.setup(context);
        }


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] data = value.toString().split(" ");
            double probThreshold = Double.parseDouble(data[0]);
            int sampleNo = Integer.parseInt(data[1]);

            int f1SampleSize = rand.nextInt(100)+1;
            int f2SampleSize = rand.nextInt(100)+1;

            randomSample(probScoreList, f1SampleSize*f2SampleSize);

            int count = 0;
            double probSum = 0;
            double scoreSum = 0;
            for (int i = 0; i < f1SampleSize*f2SampleSize; i++) {
                ProbScoreArrayList.ProbScoreTuple tuple = probScoreList.get(i);
                if (tuple.getProb() >= probThreshold ) {
                    count++;
                    probSum+= tuple.getProb();
                    scoreSum+= tuple.getZscore();
                }
            }

            sample.setCount(count);
            sample.setProbSum(probSum);
            sample.setScoreSum(scoreSum);

            probSampleNo.set("" + probThreshold + " " + sampleNo);
            context.write(probSampleNo, sample);
        }
    }

    public static void main(String args[]) throws Exception {

        Configuration conf = new Configuration();

        String[] otherArgs =
                new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: CommentWordCount <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Sample background distribution");

        job.setJarByClass(SampleBgDistribution.class);
        job.setMapperClass(SampleDistMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ProbScoreArrayList.ProbScoreTuple.class);
        job.setNumReduceTasks(0);

//        job.addCacheFile(new URI(otherArgs[2]+"#"+SampleDistMapper.PROBSCORE_FILENAME));
        job.addCacheFile(new Path(otherArgs[2]).toUri());

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
