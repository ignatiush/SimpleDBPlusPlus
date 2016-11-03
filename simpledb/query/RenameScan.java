package simpledb.query;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * The scan class corresponding to the <i>rename</i> relational
 * algebra operator.
 * Created by limjing7 on 27/10/16.
 */
public class RenameScan implements Scan {

    private Scan s;
    private Map<String, String> fieldlist;
    private Map<String, String > rev;

    /**
     * Creates a rename scan having the specified
     * underlying scan and field list.
     * @param s the underlying scan
     * @param fieldlist the list of field names
     */
    public RenameScan(Scan s, Map<String, String> fieldlist) {
        this.s = s;
        this.fieldlist = fieldlist;
        rev = fieldlist.entrySet()
              .stream()
              .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }

    public void beforeFirst() {
        s.beforeFirst();
    }

    public boolean next() {
        return s.next();
    }

    public void close() {
        s.close();
    }

    public Constant getVal(String fldname) {
        if (hasField(fldname))
            return s.getVal(rev.get(fldname));
        else
            return s.getVal(fldname);
    }

    public int getInt(String fldname) {
        if (hasField(fldname))
            return s.getInt(rev.get(fldname));
        else
            return s.getInt(fldname);
    }

    public String getString(String fldname) {
        if (hasField(fldname)){
            return s.getString(rev.get(fldname));
        }
        return s.getString(fldname);
    }

    /**
     * Returns true if the specified field
     * is in the rename map.
     * @see simpledb.query.Scan#hasField(java.lang.String)
     */
    public boolean hasField(String fldname) {
        return rev.containsKey(fldname);
    }
}
