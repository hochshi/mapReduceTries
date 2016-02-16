import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.je.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * Created by lab on 16/02/16.
 */
public class CreateProbScoreBDB {

    public static void main(String[] args) throws Exception {
        // create a configuration for DB environment
        EnvironmentConfig envConf = new EnvironmentConfig();
        // environment will be created if not exists
        envConf.setAllowCreate(true);

        // open/create the DB environment using config
        Environment dbEnv = new Environment(new File("/tmp/d2dBD"), envConf);

        DatabaseConfig dbConf = new DatabaseConfig();
        // db will be created if not exits
        dbConf.setAllowCreate(true);

        // create/open testDB using config
        Database testDB = dbEnv.openDatabase(null, "testDB", dbConf);

        // key
        DatabaseEntry key = new DatabaseEntry();
        // data
        DatabaseEntry data = new DatabaseEntry();

        ProbScoreArrayList.ProbScoreTuple tuple = new ProbScoreArrayList.ProbScoreTuple(0,0);

        int count = 1;

        try(BufferedReader br = new BufferedReader(new FileReader(args[0]))) {
            String[] tmp;
            for (String line; (line = br.readLine()) != null; ) {
                // process the line.
                tmp = line.split(" ");
                tuple.setValues(Double.parseDouble(tmp[4]), Double.parseDouble(tmp[5]));
                IntegerBinding.intToEntry(count, key);
                testDB.put(null, key, tuple.objectToEntry());
            }
        }
        // important: do not forget to close them!
        testDB.close();
        dbEnv.close();
    }

}
