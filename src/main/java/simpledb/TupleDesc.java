package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 * 用于描述tuple，tuple代表表中的一行数据，
 * 但是需要描述这一行中每一列应该是什么类型，字段叫什么
 */
public class TupleDesc implements Serializable {

    private static final long serialVersionUID = 1L;

    private Type[] typeAr;
    private String[] fieldAr;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {

        if (typeAr == null || fieldAr == null)
            throw new IllegalArgumentException("typeAr and fieldAr must not be null.");

        if (typeAr.length == 0)
            throw new IllegalArgumentException("typeAr must contain at least one entry.");

        // some code goes here
        this.typeAr = typeAr;
        this.fieldAr = fieldAr;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        if (typeAr.length == 0)
            throw new IllegalArgumentException("typeAr must contain at least one entry.");
        this.typeAr = typeAr;
        this.fieldAr = new String[typeAr.length];
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        Type[] mergeType = new Type[td1.typeAr.length + td2.typeAr.length];
        // 这玩意可能为空
        String[] mergeField = new String[mergeType.length];

        int idx = 0;
        for (int i = 0; i < td1.typeAr.length; i++) {
            mergeType[idx] = td1.typeAr[i];
            mergeField[idx] = td1.fieldAr[i];
            idx++;
        }
        for (int i = 0; i < td2.typeAr.length; i++) {
            mergeType[idx] = td2.typeAr[i];
            mergeField[idx] = td2.fieldAr[i];
            idx++;
        }

        return new TupleDesc(mergeType, mergeField);
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // some code goes here

        List<TDItem> tdItems = new ArrayList<>();

        for (int i = 0; i < this.typeAr.length; i++) {
            Type type = typeAr[i];
            String name = fieldAr[i];
            TDItem tdItem = new TDItem(type, name);
            tdItems.add(tdItem);
        }

        return tdItems.iterator();
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return fieldAr.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i > typeAr.length) throw new NoSuchElementException("i is not a valid field reference");

        return fieldAr[i];
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i > typeAr.length) throw new NoSuchElementException("i is not a valid field reference.");

        return typeAr[i];
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if (name == null) throw new NoSuchElementException("name is null");
        // some code goes here
        for (int i = 0; i < fieldAr.length; i++) {
            if (name.equals(fieldAr[i])) {
                return i;
            }
        }
        throw new NoSuchElementException("cannot find index for name [" + name + "].");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int result = 0;
        for (Type type : typeAr) {
            result = result + type.getLen();
        }

        return result;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if (!(o instanceof TupleDesc)) return false;
        if (o.hashCode() != this.hashCode()) return false;

        TupleDesc td = (TupleDesc) o;
        return this.numFields() == td.numFields() && this.getSize() == td.getSize();
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
//        throw new UnsupportedOperationException("unimplemented");
        return (this.typeAr.length << this.typeAr.length) * typeAr.length;
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
        StringBuilder sb = new StringBuilder("TupleDesc:[");

        String[] str = new String[typeAr.length];

        for (int i = 0; i < typeAr.length; i++) {
            String name = typeAr[i].name();
            String s = fieldAr[i];
            str[i] = name + "(" + (s == null ? "null" : s) + ")";
        }
        String join = String.join(",", str);
        sb.append(join);
        sb.append("]");

        return sb.toString();
    }

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        Type fieldType;

        /**
         * The name of the field
         */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
}
