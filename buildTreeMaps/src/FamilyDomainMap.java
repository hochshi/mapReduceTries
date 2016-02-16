import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Created by lab on 14/02/16.
 */
public class FamilyDomainMap {

    public static class StringComparator implements Comparator<String>, Serializable {
        @Override
        public int compare(String o1, String o2) {
            return o1.compareTo(o2);
        }
    }

    public static class NumberComparator implements Comparator<Integer>, Serializable {
        @Override
        public int compare(Integer o1, Integer o2) {
            return Integer.compare(o1, o2);
        }
    }

    public static void main(String[] args) throws Exception {
        TreeMap<String, ArrayList<Integer>> family2domain = new TreeMap(new StringComparator());

        try(BufferedReader br = new BufferedReader(new FileReader(args[0]))) {
            String[] tmp;
            for(String line; (line = br.readLine()) != null; ) {
                // process the line.
                tmp = line.split(" ");
                ArrayList<Integer> family = family2domain.get(tmp[0]);
                try {
                    family.add(Integer.parseInt(tmp[1]));
                } catch (NullPointerException exp) {
                    family = new ArrayList<Integer>();
                    family.add(Integer.parseInt(tmp[1]));
                    family2domain.put(tmp[0], family);
                }
            }
            // line is not visible here.
        }
        FileOutputStream fileOut =
                new FileOutputStream("/tmp/employee.ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(family2domain);
        out.close();
        fileOut.close();
    }
}
