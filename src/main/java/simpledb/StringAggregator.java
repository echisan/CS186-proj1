package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 * 对string类型进行聚合
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int groupByFieldNum;
    private Type groupByType;
    private int aggsFieldNum;
    private Op op;
    private List<Tuple> nogroupby;
    private Map<Field, List<StringField>> stringFieldFieldMap;
    private boolean isgroup = true;
    private TupleDesc tupleDesc;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (!what.equals(Op.COUNT)) {
            throw new IllegalArgumentException("only support COUNT");
        }
        // some code goes here
        this.groupByFieldNum = gbfield;
        this.groupByType = gbfieldtype;
        this.aggsFieldNum = afield;
        this.op = what;
        if (gbfield == NO_GROUPING || gbfieldtype == null) {
            isgroup = false;
            nogroupby = new ArrayList<>();
        } else {
            stringFieldFieldMap = new HashMap<>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (tupleDesc == null) {
            tupleDesc = tup.getTupleDesc();
        }
        // some code goes here
        if (!isgroup) {
            nogroupby.add(tup);
            return;
        }
        List<StringField> stringFields = stringFieldFieldMap.get(tup.getField(groupByFieldNum));
        if (stringFields == null) {
            stringFields = new ArrayList<>();
        }
        stringFields.add((StringField) tup.getField(aggsFieldNum));
        stringFieldFieldMap.put(tup.getField(groupByFieldNum), stringFields);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return isgroup ? groupByIterator() : noGroupIterator();
    }

    private DbIterator groupByIterator() {
        TupleDesc tupleDesc = new TupleDesc(new Type[]{groupByType, Type.INT_TYPE}, new String[]{this.tupleDesc.getFieldName(groupByFieldNum), op.toString()});
        List<Tuple> tuples = new ArrayList<>();
        stringFieldFieldMap.forEach((key, value) -> {
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, key);
            tuple.setField(1, new IntField(value.size()));
            tuples.add(tuple);
        });
        return new TupleIterator(tupleDesc, tuples);
    }

    private DbIterator noGroupIterator() {
        Tuple tuple = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{op.toString()}));
        tuple.setField(0, new IntField(nogroupby.size()));
        return new TupleIterator(tupleDesc, Collections.singleton(tuple));
    }
}
