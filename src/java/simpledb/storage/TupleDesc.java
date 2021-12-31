package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

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
    private ArrayList<TDItem> tdItems;
    private int typeNums;

    private int nullFiledNames;
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
        typeNums = typeAr.length;
        tdItems = new ArrayList<TDItem>(typeNums);
        for(int i = 0; i < typeNums; i++){
            tdItems.add(new TDItem(typeAr[i],i >= fieldAr.length ? null : fieldAr[i]));
            if(i >= fieldAr.length){
                nullFiledNames ++;
            }
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
        typeNums = typeAr.length;
        tdItems = new ArrayList<TDItem>(typeNums);
        for(int i = 0; i < typeNums; i++){
            tdItems.add(new TDItem(typeAr[i], null));
            nullFiledNames ++;
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.typeNums;
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
        if(i >= 0 && i < typeNums){
            return tdItems.get(i).fieldName;
        }
        throw new NoSuchElementException("没有指定的FieldName ***** from TupleDesc.getFieldName(i)");
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
        if(i >= 0 && i < typeNums){
            return tdItems.get(i).fieldType;
        }
        throw new NoSuchElementException("没有指定的FieldType ***** from TupleDesc.getFieldType(i)");
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
        if(name != null && nullFiledNames != typeNums){
            for(int i = 0; i < typeNums; i++){
                if(tdItems.get(i).fieldName.equals(name)){
                    return i;
                }
            }
        }
        throw new NoSuchElementException("没有找到给定Name的TdItems ***** from TupleDesc.fieldNameToIndex");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for(int i = 0; i < typeNums; i++){
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
        int size1 = td1.typeNums;
        int size2 = td2.typeNums;
        Type[] typeAr = new Type[size1 + size2];
        String[] fieldAr = new String[size1 + size2];
        int index = 0;
        for(TDItem t : td1.tdItems){
            typeAr[index] = t.fieldType;
            fieldAr[index ++] = t.fieldName;
        }
        for(TDItem t : td2.tdItems){
            typeAr[index] = t.fieldType;
            fieldAr[index++] = t.fieldName;
        }
        return new TupleDesc(typeAr, fieldAr);
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
        if(o instanceof TupleDesc){
            if(this.typeNums == ((TupleDesc) o).typeNums){
                for(int i = 0; i < typeNums; i++){
                    if(!this.tdItems.get(i).fieldType.equals(((TupleDesc) o).tdItems.get(i).fieldType)){
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
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
        String res = "";
        for(int i = 0; i < typeNums - 1; i++){
            res += tdItems.get(i).toString() + ",";
        }
        res += tdItems.get(typeNums - 1).toString();
        return res;
    }
}
