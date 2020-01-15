package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 * 对int类型进行聚合，也就是说，sum(int字段)
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupByFieldNum;
    private Type groupByFieldType;
    private int aggregateFieldNum;
    private Op op;
    private Map<Field, List<IntField>> fieldListMap;
    private TupleDesc tupleDesc;

    private List<IntField> noGroupFields;
    private boolean isgroup = false;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.groupByFieldNum = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregateFieldNum = afield;
        this.op = what;
        // no group
        if (gbfield == NO_GROUPING || gbfieldtype == null) {
            noGroupFields = new ArrayList<>();
        } else {
            fieldListMap = new HashMap<>();
            isgroup = true;
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (tupleDesc == null) {
            tupleDesc = tup.getTupleDesc();
        }
        // 如果不是group的话
        if (!isgroup) {
            this.noGroupFields.add((IntField) tup.getField(aggregateFieldNum));
            return;
        }

        // group by
        List<IntField> fields = fieldListMap.get(tup.getField(groupByFieldNum));
        if (fields == null) {
            fields = new ArrayList<>();
        }
        fields.add((IntField) tup.getField(aggregateFieldNum));

        fieldListMap.put(tup.getField(groupByFieldNum), fields);
    }


    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public DbIterator iterator() {
        return isgroup ? groupIterator() : noGroupIterator();
    }

    private DbIterator noGroupIterator() {
        TupleDesc tupleDesc = new TupleDesc(new Type[]{this.tupleDesc.getFieldType(aggregateFieldNum)}, new String[]{op.toString()});
        Tuple tuple = new Tuple(tupleDesc);
        switch (op) {
            case MIN: {
                int min = -1;
                for (IntField noGroupField : noGroupFields) {
                    if (min == -1 || noGroupField.getValue() < min) {
                        min = noGroupField.getValue();
                    }
                }
                tuple.setField(0, new IntField(min));
                break;
            }
            case AVG: {
                int sum = 0;
                for (IntField noGroupField : noGroupFields) {
                    sum = sum + noGroupField.getValue();
                }
                tuple.setField(0, new IntField(sum / noGroupFields.size()));
                break;
            }
            case SUM: {
                int sum = 0;
                for (IntField noGroupField : noGroupFields) {
                    sum = sum + noGroupField.getValue();
                }
                tuple.setField(0, new IntField(sum));
                break;
            }
            case MAX: {
                int max = 0;
                for (IntField noGroupField : noGroupFields) {
                    if (noGroupField.getValue() > max) {
                        max = noGroupField.getValue();
                    }
                }
                tuple.setField(0, new IntField(max));
                break;
            }
            case COUNT: {
                tuple.setField(0, new IntField(noGroupFields.size()));
            }
            default:
                throw new UnsupportedOperationException();
        }
        return new TupleIterator(tupleDesc, Collections.singleton(tuple));
    }

    private DbIterator groupIterator() {
        TupleDesc td = new TupleDesc(new Type[]{groupByFieldType, Type.INT_TYPE}, new String[]{tupleDesc.getFieldName(groupByFieldNum), op.toString()});
        List<Tuple> list = new ArrayList<>();
        fieldListMap.forEach((key, value) -> {
            Tuple tuple = new Tuple(td);
            tuple.setField(0, key);
            switch (op) {
                case COUNT: {
                    tuple.setField(1, new IntField(value.size()));
                    list.add(tuple);
                    break;
                }
                case MAX: {
                    int max = 0;
                    for (IntField field : value) {
                        if (field.getValue() > max) {
                            max = field.getValue();
                        }
                    }
                    tuple.setField(1, new IntField(max));
                    list.add(tuple);
                    break;
                }
                case SUM: {
                    int sum = 0;
                    for (Field field : value) {
                        IntField intField = (IntField) field;
                        sum = sum + intField.getValue();
                    }
                    tuple.setField(1, new IntField(sum));
                    list.add(tuple);
                    break;
                }
                case AVG:
                    int sum = 0;
                    for (Field field : value) {
                        IntField intField = (IntField) field;
                        sum = sum + intField.getValue();
                    }
                    tuple.setField(1, new IntField(sum / value.size()));
                    list.add(tuple);
                    break;
                case MIN:
                    int min = -1;
                    for (Field field : value) {
                        IntField intField = (IntField) field;
                        if (min == -1 || intField.getValue() < min) {
                            min = intField.getValue();
                        }
                    }
                    tuple.setField(1, new IntField(min));
                    list.add(tuple);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        });
        // some code goes here
        return new TupleIterator(td, list);
    }
}
