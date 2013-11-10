package com.etleap.cascading.topn;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.assembly.AggregateBy.Functor;
import cascading.pipe.assembly.AggregateBySketch;
import cascading.tuple.*;
import cascading.tuple.hadoop.SerializationToken;
import cascading.tuple.hadoop.io.BufferedInputStream;
import cascading.tuple.util.TupleViews;
import com.akeera.collections.SpaceSavingTopN;
import org.apache.hadoop.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Class CompositeFunction takes multiple Functor instances and manages them as a single {@link cascading.operation.Function}.
 *
 * @see cascading.pipe.assembly.AggregateBy.Functor
 */
public class TopNFunction extends BaseOperation<TopNFunction.Context> implements Function<TopNFunction.Context>
{

    private static final Logger LOG = LoggerFactory.getLogger(AggregateBySketch.class);

    private int topK = 0;
    private final Fields topNFields;


    public enum Flush
    {
        Num_Keys_Flushed
    }

    public static class Context
    {
        SpaceSavingTopN<Tuple> topN;
        TupleEntry[] arguments;
        Tuple result;
    }

    /**
     * Constructor CompositeFunction creates a new CompositeFunction instance.
     *
     * @param topNFields of type Fields
     * @param topK      of type int
     */
    public TopNFunction(Fields topNFields, int topK)
    {
        super( topNFields );
        this.topNFields = topNFields;
        this.topK = topK;
    }


    @Override
    public void prepare( final FlowProcess flowProcess, final OperationCall<Context> operationCall )
    {
        Fields[] fields = new Fields[2];
        fields[0] = new Fields("key");
        fields[1] = new Fields("count");

        final Context context = new Context();
        context.topN = new SpaceSavingTopN<Tuple>(topK);

        context.result = TupleViews.createComposite( fields );
        operationCall.setContext( context );
    }

    @Override
    public void operate( FlowProcess flowProcess, FunctionCall<TopNFunction.Context> functionCall )
    {
        TupleEntry arguments = functionCall.getArguments();
        Tuple key   = arguments.selectTupleCopy(new Fields("word"));
        Context context = functionCall.getContext();
        context.topN.add(key);
    }


    @Override
    public void flush( FlowProcess flowProcess, OperationCall<TopNFunction.Context> operationCall )
    {
        // need to drain context
        TupleEntryCollector collector = ( (FunctionCall) operationCall ).getOutputCollector();

        Tuple result = operationCall.getContext().result;

        SpaceSavingTopN<Tuple> topN = operationCall.getContext().topN;

        for(Tuple tuple : topN.getElementIndex().keySet()){


            Tuple countTuple = new Tuple();
            IntWritable  count = new IntWritable();
            count.set(topN.getCountByElement(tuple));
            countTuple.add(count.get());

            Text text = new Text();
            text.set(tuple.getString(0));


            Tuple keyTuple = new Tuple();
            CountTupleKey key = new CountTupleKey();
            key.set(count,text);
            keyTuple.add(key);



            Tuple countResult =  new Tuple();
            countResult.add(key);
            countResult.add(count);


            //TupleViews.reset(result,countResult);
            collector.add(countResult);
        }

        operationCall.setContext( null );
    }


    static class CountTupleKey implements WritableComparable<CountTupleKey> , Serializable{
        Text value = new Text();
        IntWritable count = new IntWritable();


        public void set(IntWritable count, Text value) {
            this.count = count;
            this.value = value;
        }

        @Override
        public void readFields(DataInput in) throws IOException {

            value.readFields(in);
            count.set(in.readInt());
        }


        @Override
        public void write(DataOutput out) throws IOException {
            value.write(out);
            count.write(out);

        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        public static class Comparator extends  WritableComparator {
            public Comparator() {
                super(CountTupleKey.class);
            }

            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                try {
                    int val1 = WritableComparator.readInt(b1, s1);
                    int val2 = WritableComparator.readInt(b2, s2);
                    if(val1 != val2) {
                        return val1 > val2 ? 1 : -1;
                    }
                    int offset1 = s1 + 4;
                    int offset2 = s2 + 4;
                    int strSize1 = WritableComparator.readVInt(b1, offset1);
                    int strSize2 = WritableComparator.readVInt(b2, offset2);
                    offset1 += WritableUtils.decodeVIntSize(b1[offset1]);
                    offset2 += WritableUtils.decodeVIntSize(b2[offset2]);
                    int cmp = WritableComparator.compareBytes(b1, offset1, strSize1, b2, offset2, strSize2);
                    if(cmp != 0) {
                        return cmp;
                    }
                    offset1 += strSize1;
                    offset2 += strSize2;
                    long f1 = WritableComparator.readLong(b1, offset1);
                    long f2 = WritableComparator.readLong(b2, offset2);
                    if(f1 != f2) {
                        return f1 > f2 ? 1 : -1;
                    }
                    return 0;
                } catch(IOException e) {
                    throw new RuntimeException(e);
                }
            }

        }

        static { // register this comparator
            WritableComparator.define(CountTupleKey.class, new Comparator());
        }



        public boolean equals(Object object){
            if( this == object )
                return true;
            if( !( object instanceof CountTupleKey) )
                return false;
            if( !super.equals( object ) )
                return false;

            CountTupleKey that = (CountTupleKey)object;
            return (value.equals(that.value) && count.equals(that.count));
        }

        @Override
        public int compareTo(CountTupleKey o) {
            if(o.count.equals(count)){
                return value.compareTo(o.value);
            }else{
                return o.count.compareTo(count);
            }
        }

        @Override
        public String toString(){
            return  value.toString() + "\t" + count;
        }
    }



    @Override
    public boolean equals( Object object )
    {
        if( this == object )
            return true;
        if( !( object instanceof TopNFunction) )
            return false;
        if( !super.equals( object ) )
            return false;

        TopNFunction that = (TopNFunction) object;

        if( topK != that.topK)
            return false;
        if( !topNFields.equals(that.topNFields))
            return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + topK;
        result = 31 * result + ( topNFields != null ? topNFields.hashCode() : 0 );
        return result;
    }
}