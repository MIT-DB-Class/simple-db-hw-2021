package simpledb.storage;

import java.io.Serializable;
import java.util.*;
import java.util.Iterator;
import simpledb.common.Type;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private RecordId recordId;
    private TupleDesc tupleDesc;
    private ArrayList<Field> fields;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        this.tupleDesc = td;
        this.fields = new ArrayList<>();
        initFields(td);
    }
    public void initFields(TupleDesc td){
        for(int i=0;i<td.numFields();i++){
            Type type = td.getFieldType(i);
            if(type == Type.INT_TYPE){
                fields.add(new IntField(0));
            }else if(type == Type.STRING_TYPE){
                fields.add(new StringField("", type.getLen()));
            }
        }
    }
    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        fields.set(i,f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        return fields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        Iterator<TupleDesc.TDItem> tdItems = this.tupleDesc.iterator();
        int i = 0;
        while (tdItems.hasNext()) {
            TupleDesc.TDItem item = tdItems.next();
            stringBuilder.append("FiledName: ").append(item.fieldName);
            stringBuilder.append("==> Value: ").append(fields.get(i).toString());
            stringBuilder.append("\n");
            i++;
        }
        return stringBuilder.toString();
        //        return "TEST";
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return fields.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        initFields(td);
    }
}
