import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Created by lab on 14/02/16.
 */
public class D2DProbScoreMap {
    public static void main(String[] args) throws Exception {
//        TreeMap<Integer, TreeMap<Integer, HashMap<String, Float>>> probScoreMap = new TreeMap<>(new FamilyDomainMap.NumberComparator());
        TreeMap<Integer, TreeMap<Integer, String>> probScoreMap = new TreeMap<>(new FamilyDomainMap.NumberComparator());

        try(BufferedReader br = new BufferedReader(new FileReader(args[0]))) {
            String[] tmp;
            for(String line; (line = br.readLine()) != null; ) {
                // process the line.
                tmp = line.split(" ");
                TreeMap<Integer, String> d2d = probScoreMap.get(Integer.parseInt(tmp[0]));
                try {
                    d2d.put(Integer.parseInt(tmp[1]), "" + tmp[4]+" "+tmp[5]);
                } catch (NullPointerException exp) {
                    d2d = new TreeMap<>(new FamilyDomainMap.NumberComparator());
                    d2d.put(Integer.parseInt(tmp[1]), "" + tmp[4]+" "+tmp[5]);
                    probScoreMap.put(Integer.parseInt(tmp[0]), d2d);
                }
            }
            // line is not visible here.
        }
        FileOutputStream fileOut =
                new FileOutputStream("/tmp/employee.ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(probScoreMap);
        out.close();
        fileOut.close();
    }
}
