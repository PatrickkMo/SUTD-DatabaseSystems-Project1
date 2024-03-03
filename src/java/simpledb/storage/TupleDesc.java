package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private int fieldNum;
    private TDItem[] fieldItems;
    private HashMap<String, Integer> fieldMap;
    
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
        List<TDItem> tdItemList = Arrays.asList(fieldItems);
        return (Iterator<TDItem>) tdItemList.iterator();
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
        this.fieldNum = typeAr.length;
        this.fieldItems = new TDItem[fieldNum];
        this.fieldMap = new HashMap<>();

        if (typeAr.length == 0) {
            throw new IllegalArgumentException("typeAr must contain at least one entry");
        }

        if (typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException("Length mismatch between typeAr and fieldAr");
        }

        for (int i = 0; i < fieldNum; i++) {
            fieldItems[i] = new TDItem(typeAr[i], fieldAr[i]);
            fieldMap.put(fieldAr[i], i); // map field name to index
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
        this(typeAr, new String[typeAr.length]);
    }

    public TupleDesc(TDItem[] items) {
        this.fieldItems = items;
        this.fieldNum = items.length;
        this.fieldMap = new HashMap<>();
        for (int i = 0; i < fieldNum; i++) {
            fieldMap.put(items[i].fieldName, i);
        }

    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.fieldNum;
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
        if (!(i >= 0 && i < fieldNum)) { // check if i is a valid index
            throw new NoSuchElementException("i is not a valid field reference");
        }
        
        return fieldItems[i].fieldName;
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
        if (!(i >= 0 && i < fieldNum)) { // check if i is a valid index
            throw new NoSuchElementException("i is not a valid field reference");
        }
        return fieldItems[i].fieldType;
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
        if (!fieldMap.containsKey(name)) {
            throw new NoSuchElementException("No field with a matching name is found");
        }
        return fieldMap.get(name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (TDItem item : fieldItems) {
            size += item.fieldType.getLen();
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
        TDItem[] firItems = td1.fieldItems;
        TDItem[] secItems = td2.fieldItems;

        int firLen = firItems.length;
        int secLen = secItems.length;
        int totalLen = firLen + secLen;
        TDItem[] newItems = new TDItem[totalLen];
        
        System.arraycopy(firItems, 0, newItems, 0, firLen);
        System.arraycopy(secItems, 0, newItems, firLen, secLen);
        return new TupleDesc(newItems);
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
        if (this == o || (o == null && this == null)) {
            return true;
        }
        else if ((o == null && this != null) || !(o instanceof TupleDesc) || (o != null && this == null)) {
            return false;
        }
        else {
            TupleDesc other = (TupleDesc) o;
            if (this.numFields() == other.fieldNum) {
                for (int i = 0; i < fieldNum; i++) {
                    if (fieldItems[i].fieldType.equals(other.fieldItems[i].fieldType)) {
                        return true;
                    }
                }
            }
            
            return false;
        }
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
        String descString = "";

        for (int i = 0; i < fieldItems.length - 1; i++) {
            descString += fieldItems[i].fieldType + "(" + fieldItems[i].fieldName + "), ";
        }
        descString += fieldItems[fieldItems.length - 1].fieldType + "(" + fieldItems[fieldItems.length - 1].fieldName + ")";
        return descString;
    }
}
