import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;

/**
 * Created by lab on 15/02/16.
 */
public class ReadJson {

    public static void main(String args[]) throws Exception {
        FileInputStream fileIn = new FileInputStream("/tmp/employee.ser");
        Reader in = new BufferedReader(new InputStreamReader(fileIn));
        Gson gson = new Gson();
        ArrayList<ProbScoreArrayList.ProbScoreTuple> probScoreList;
        probScoreList = gson.fromJson(in, new TypeToken<ArrayList<ProbScoreArrayList.ProbScoreTuple>>() {}.getType());

        System.out.println("Testing");
    }
}
