package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A LogicalFilterNode represents the parameters of a filter in the WHERE clause of a query.
    <p>
    Filter is of the form t.f p c
    <p>
    Where t is a table, f is a field in t, p is a predicate, and c is a constant
*/
public class LogicalFilterNode {
    /** The alias of a table (or the name if no alias) over which the filter ranges */
    public final String tableAlias;

    /** The predicate in the filter */
    public final Predicate.Op p;
    
    /* The constant on the right side of the filter */
    public final String c;
    
    /** The field from t which is in the filter. The pure name, without alias or tablename*/
    public final String fieldPureName;
    
    public final String fieldQuantifiedName;
    
    public LogicalFilterNode(String table, String field, Predicate.Op pred, String constant) {
        tableAlias = table;
        p = pred;
        c = constant;
        String[] tmps = field.split("[.]");
        if (tmps.length>1)
            fieldPureName = tmps[tmps.length-1];
        else
            fieldPureName=field;
        this.fieldQuantifiedName = tableAlias+"."+fieldPureName;
    }
}
