import com.google.gson.Gson;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseEntry;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by lab on 15/02/16.
 */
public class ProbScoreArrayList {

    public static class ProbScoreTuple implements Serializable {
        private double zscore = 0;
        private double prob = 0;

        public ProbScoreTuple(double prob, double zscore) {
            this.zscore = zscore;
            this.prob = prob;
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

        public void setValues(double prob, double zscore) {
            this.zscore = zscore;
            this.prob = prob;
        }

        public DatabaseEntry objectToEntry() {

            TupleOutput output = new TupleOutput();

            DatabaseEntry entry = new DatabaseEntry();

            output.writeDouble(prob);
            output.writeDouble(zscore);
            TupleBinding.outputToEntry(output, entry);

            return entry;
        }

        public void entryToObject(DatabaseEntry entry) {

            TupleInput input = TupleBinding.entryToInput(entry);
            setProb(input.readDouble());
            setZscore(input.readDouble());
        }

        }

    public static void main(String[] args) throws Exception {
        ArrayList<ProbScoreTuple> list = new ArrayList<>();

        try(BufferedReader br = new BufferedReader(new FileReader(args[0]))) {
            String[] tmp;
            for(String line; (line = br.readLine()) != null; ) {
                // process the line.
                tmp = line.split(" ");
                list.add(new ProbScoreTuple(Double.parseDouble(tmp[4]), Double.parseDouble(tmp[5])));
            }
            // line is not visible here.
        }
        FileOutputStream fileOut =
                new FileOutputStream("/tmp/employee.ser");
        /*ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(list);
        out.close();
        fileOut.close();*/
        Writer out = new BufferedWriter(new OutputStreamWriter(fileOut));
        Gson gson = new Gson();
        gson.toJson(list, out);
        out.close();
        fileOut.close();
    }

}
