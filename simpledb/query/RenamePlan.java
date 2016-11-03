package simpledb.query;

import simpledb.record.Schema;

import java.util.Map;

/**
 * The Plan class corresponding to the <i>rename</i>
 * relational algebra operator.
 * Created by limjing7 on 27/10/16.
 */
public class RenamePlan implements Plan {
    private Plan p;
    private Schema schema = new Schema();
    private Map<String, String> as;

    /**
     * Creates a new project node in the query tree,
     * having the specified subquery and field list.
     * @param p the subquery
     * @param fieldlist the list of fields
     */
    public RenamePlan(Plan p, Map<String, String> fieldlist) {
        this.p = p;
        this.as = fieldlist;
        Schema temp = p.schema();
        for (String fldname : temp.fields()){
            if (as.containsKey(fldname)){
                schema.addField(as.get(fldname), temp.type(fldname), temp.length(fldname));
            }
            else{
                schema.addField(fldname, temp.type(fldname), temp.length(fldname));
            }
            System.out.println(fldname);
        }
    }

    /**
     * Creates a project scan for this query.
     * @see simpledb.query.Plan#open()
     */
    public Scan open() {
        Scan s = p.open();
        return new RenameScan(s, as);
    }

    /**
     * Estimates the number of block accesses in the projection,
     * which is the same as in the underlying query.
     * @see simpledb.query.Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        return p.blocksAccessed();
    }

    /**
     * Estimates the number of output records in the projection,
     * which is the same as in the underlying query.
     * @see simpledb.query.Plan#recordsOutput()
     */
    public int recordsOutput() {
        return p.recordsOutput();
    }

    /**
     * Estimates the number of distinct field values
     * in the projection,
     * which is the same as in the underlying query.
     * @see simpledb.query.Plan#distinctValues(java.lang.String)
     */
    public int distinctValues(String fldname) {
        return p.distinctValues(fldname);
    }

    /**
     * Returns the schema of the projection,
     * which is taken from the field list.
     * @see simpledb.query.Plan#schema()
     */
    public Schema schema() {
        return schema;
    }
}
