package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.nio.channels.NoConnectionPendingException;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    public ArrayList<TDItem> tdItems = new ArrayList<>();
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return tdItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        for(int i=0;i<typeAr.length; i++){
            TDItem item = new TDItem(typeAr[i], fieldAr[i]);
            tdItems.add(item);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        for(int i=0;i<typeAr.length;i++){
            TDItem item = new TDItem(typeAr[i], null);
            tdItems.add(item);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tdItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i<0 || i>tdItems.size()){
            throw new NoSuchElementException("Index out of Range." + "The length of this TupleDesc is: " + tdItems.size());
        }
        return tdItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if(i<0 || i>tdItems.size()){
            throw new NoSuchElementException("Index out of Range." + "The length of this TupleDesc is: " + tdItems.size());
        }
        return tdItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        int res = 0;
        boolean flag = false;
        for(int i=0;i<tdItems.size();i++){
            if(tdItems.get(i).fieldName == name){
                res = 0;
                flag = true;
                break;
            }
        }
        if(flag){
            return res;
        }
        else{
            throw new NoSuchElementException("There is no corresponding element which name is :" + name);
        }
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for(int i=0;i<tdItems.size();i++){
            size += tdItems.get(i).fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int lenOfNewTd = td1.numFields() + td2.numFields();
        Type[] types = new Type[lenOfNewTd];
        String[] names = new String[lenOfNewTd];
        for(int i=0;i<td1.numFields();i++){
            types[i] = td1.getFieldType(i);
            names[i] = td1.getFieldName(i);
        }
        int td1Len = td1.numFields();
        for(int i=0;i<td2.numFields();i++){
            types[td1Len + i] = td2.getFieldType(i);
            names[td1Len + i] = td2.getFieldName(i);
        }
        return new TupleDesc(types, names);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if(!(o instanceof TupleDesc)){
            return false;
        }
        TupleDesc other = (TupleDesc) o;
        if(other.numFields() != this.numFields() || other.getSize() != this.getSize()){
            return false;
        }
        for(int i=0;i<other.numFields();i++){
            if(!this.getFieldType(i).equals(other.getFieldType(i))){
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        int res = 0;
        for(TDItem item: tdItems){
            res += item.toString().hashCode()*17;
        }
        return res;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<this.numFields();i++){
            TDItem item = this.tdItems.get(i);
            sb.append(item.fieldType.toString())
                .append("[").append(i).append("]")
                .append("(").append(item.fieldName)
                .append("[").append(i).append("])");
        }
        return sb.toString();
    }
}
