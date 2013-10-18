package cascading.pipe.assembly;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.assembly.AggregateBy;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.util.TupleViews;
import cascading.pipe.assembly.AggregateBy.Functor;
import com.akeera.collections.Bucket;
import com.akeera.collections.SpaceSavingTopN;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Class CompositeFunction takes multiple Functor instances and manages them as a single {@link cascading.operation.Function}.
 *
 * @see Functor
 */
public class CompositeFunction extends BaseOperation<CompositeFunction.Context> implements Function<CompositeFunction.Context>
{

    private static final Logger LOG = LoggerFactory.getLogger(AggregateBySketch.class);

    private int topK = 0;
    private final Fields groupingFields;
    private final Fields[] argumentFields;
    private final Fields[] functorFields;
    private final AggregateBy.Functor[] functors;

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
     * @param groupingFields of type Fields
     * @param argumentFields of type Fields
     * @param functor        of type Functor
     * @param topK      of type int
     */
    public CompositeFunction( Fields groupingFields, Fields argumentFields, Functor functor, int topK )
    {
        this( groupingFields, Fields.fields( argumentFields ), new Functor[]{functor}, topK );
    }

    /**
     * Constructor CompositeFunction creates a new CompositeFunction instance.
     *
     * @param groupingFields of type Fields
     * @param argumentFields of type Fields[]
     * @param functors       of type Functor[]
     * @param topK      of type int
     */
    public CompositeFunction( Fields groupingFields, Fields[] argumentFields, Functor[] functors, int topK )
    {
        super( getFields( groupingFields, functors ) ); // todo: groupingFields should lookup incoming type information
        this.groupingFields = groupingFields;
        this.argumentFields = argumentFields;
        this.functors = functors;
        this.topK = topK;

        this.functorFields = new Fields[ functors.length ];

        for( int i = 0; i < functors.length; i++ )
            this.functorFields[ i ] = functors[ i ].getDeclaredFields();
    }

    private static Fields getFields( Fields groupingFields, Functor[] functors )
    {
        Fields fields = groupingFields;

        for( Functor functor : functors )
            fields = fields.append( functor.getDeclaredFields() );

        return fields;
    }

    @Override
    public void prepare( final FlowProcess flowProcess, final OperationCall<Context> operationCall )
    {

        Fields[] fields = new Fields[ functors.length + 1 ];

        fields[ 0 ] = groupingFields;

        for( int i = 0; i < functors.length; i++ )
            fields[ i + 1 ] = functors[ i ].getDeclaredFields();

        final Context context = new Context();

        context.arguments = new TupleEntry[ functors.length ];

        for( int i = 0; i < context.arguments.length; i++ )
        {
            Fields resolvedArgumentFields = operationCall.getArgumentFields();

            int[] pos;

            if( argumentFields[ i ].isAll() )
                pos = resolvedArgumentFields.getPos();
            else
                pos = resolvedArgumentFields.getPos( argumentFields[ i ] ); // returns null if selector is ALL

            Tuple narrow = TupleViews.createNarrow(pos);

            Fields currentFields;

            if( this.argumentFields[ i ].isSubstitution() )
                currentFields = resolvedArgumentFields.select( this.argumentFields[ i ] ); // attempt to retain comparator
            else
                currentFields = Fields.asDeclaration( this.argumentFields[ i ] );

            context.arguments[ i ] = new TupleEntry( currentFields, narrow );
        }

        context.result = TupleViews.createComposite( fields );

        context.topN = new SpaceSavingTopN<Tuple>(topK);


        operationCall.setContext( context );
    }

    @Override
    public void operate( FlowProcess flowProcess, FunctionCall<CompositeFunction.Context> functionCall )
    {
        TupleEntry arguments = functionCall.getArguments();
        Tuple key = arguments.selectTupleCopy( groupingFields );
        Context context = functionCall.getContext();
        context.topN.add(key);
    }


    @Override
    public void flush( FlowProcess flowProcess, OperationCall<CompositeFunction.Context> operationCall )
    {
        // need to drain context
        TupleEntryCollector collector = ( (FunctionCall) operationCall ).getOutputCollector();

        Tuple result = operationCall.getContext().result;

        SpaceSavingTopN<Tuple> topN = operationCall.getContext().topN;

        for(Tuple tuple : topN.getElementIndex().keySet()){
            Tuple[] results = new Tuple[ functors.length + 1 ];
            results[ 0 ] = tuple;
            int count = topN.getCountByElement(tuple);
            Tuple countResult =  new Tuple();
            countResult.add(count);
            results[ 1 ] = countResult;
            TupleViews.reset( result, results );
            collector.add( result );
        }

        operationCall.setContext( null );
    }

//    private void completeFunctors( FlowProcess flowProcess, TupleEntryCollector outputCollector, Tuple result, Map.Entry<Tuple, Tuple[]> entry )
//    {
//        Tuple[] results = new Tuple[ functors.length + 1 ];
//
//        results[ 0 ] = entry.getKey();
//
//        Tuple[] values = entry.getValue();
//
//        for( int i = 0; i < functors.length; i++ )
//            results[ i + 1 ] = functors[ i ].complete( flowProcess, values[ i ] );
//
//        TupleViews.reset( result, results );
//
//        outputCollector.add( result );
//    }

    @Override
    public boolean equals( Object object )
    {
        if( this == object )
            return true;
        if( !( object instanceof CompositeFunction ) )
            return false;
        if( !super.equals( object ) )
            return false;

        CompositeFunction that = (CompositeFunction) object;

        if( topK != that.topK)
            return false;
        if( !Arrays.equals(argumentFields, that.argumentFields) )
            return false;
        if( !Arrays.equals( functorFields, that.functorFields ) )
            return false;
        if( !Arrays.equals( functors, that.functors ) )
            return false;
        if( groupingFields != null ? !groupingFields.equals( that.groupingFields ) : that.groupingFields != null )
            return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + topK;
        result = 31 * result + ( groupingFields != null ? groupingFields.hashCode() : 0 );
        result = 31 * result + ( argumentFields != null ? Arrays.hashCode( argumentFields ) : 0 );
        result = 31 * result + ( functorFields != null ? Arrays.hashCode( functorFields ) : 0 );
        result = 31 * result + ( functors != null ? Arrays.hashCode( functors ) : 0 );
        return result;
    }
}