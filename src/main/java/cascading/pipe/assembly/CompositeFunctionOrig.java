package cascading.pipe.assembly;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.assembly.AggregateBy.Functor;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.util.TupleViews;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Class CompositeFunction takes multiple Functor instances and manages them as a single {@link cascading.operation.Function}.
 *
 * @see cascading.pipe.assembly.AggregateBy.Functor
 */
public class CompositeFunctionOrig extends BaseOperation<CompositeFunctionOrig.Context> implements Function<CompositeFunctionOrig.Context>
{

    private static final Logger LOG = LoggerFactory.getLogger(AggregateBySketch.class);

    private int topK = 0;
    private final Fields groupingFields;
    private final Fields[] argumentFields;
    private final Fields[] functorFields;
    private final Functor[] functors;

    public enum Flush
    {
        Num_Keys_Flushed
    }

    public static class Context
    {
        LinkedHashMap<Tuple, Tuple[]> lru;

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
    public CompositeFunctionOrig(Fields groupingFields, Fields argumentFields, Functor functor, int topK)
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
    public CompositeFunctionOrig(Fields groupingFields, Fields[] argumentFields, Functor[] functors, int topK)
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

        context.lru = new LinkedHashMap<Tuple, Tuple[]>(topK, 0.75f, true )
        {
            long flushes = 0;

            @Override
            protected boolean removeEldestEntry( Map.Entry<Tuple, Tuple[]> eldest )
            {
                boolean doRemove = size() > topK;

                if( doRemove )
                {
                    completeFunctors( flowProcess, ( (FunctionCall) operationCall ).getOutputCollector(), context.result, eldest );
                    flowProcess.increment( Flush.Num_Keys_Flushed, 1 );

                    if( flushes % topK == 0 ) // every multiple, write out data
                    {
                        Runtime runtime = Runtime.getRuntime();
                        long freeMem = runtime.freeMemory() / 1024 / 1024;
                        long maxMem = runtime.maxMemory() / 1024 / 1024;
                        long totalMem = runtime.totalMemory() / 1024 / 1024;

                        LOG.info( "flushed keys num times: {}, with topK: {}", flushes + 1, topK);
                        LOG.info( "mem on flush (mb), free: " + freeMem + ", total: " + totalMem + ", max: " + maxMem );

                        float percent = (float) totalMem / (float) maxMem;

                    }

                    flushes++;
                }

                return doRemove;
            }
        };

        operationCall.setContext( context );
    }

    @Override
    public void operate( FlowProcess flowProcess, FunctionCall<CompositeFunctionOrig.Context> functionCall )
    {
        TupleEntry arguments = functionCall.getArguments();
        Tuple key = arguments.selectTupleCopy( groupingFields );

        Context context = functionCall.getContext();
        Tuple[] functorContext = context.lru.get( key );

        if( functorContext == null )
        {
            functorContext = new Tuple[ functors.length ];
            context.lru.put( key, functorContext );
        }

        for( int i = 0; i < functors.length; i++ )
        {
            TupleViews.reset( context.arguments[ i ].getTuple(), arguments.getTuple() );
            functorContext[ i ] = functors[ i ].aggregate( flowProcess, context.arguments[ i ], functorContext[ i ] );
        }
    }

    @Override
    public void flush( FlowProcess flowProcess, OperationCall<CompositeFunctionOrig.Context> operationCall )
    {
        // need to drain context
        TupleEntryCollector collector = ( (FunctionCall) operationCall ).getOutputCollector();

        Tuple result = operationCall.getContext().result;
        LinkedHashMap<Tuple, Tuple[]> context = operationCall.getContext().lru;

        for( Map.Entry<Tuple, Tuple[]> entry : context.entrySet() )
            completeFunctors( flowProcess, collector, result, entry );

        operationCall.setContext( null );
    }

    private void completeFunctors( FlowProcess flowProcess, TupleEntryCollector outputCollector, Tuple result, Map.Entry<Tuple, Tuple[]> entry )
    {
        Tuple[] results = new Tuple[ functors.length + 1 ];

        results[ 0 ] = entry.getKey();

        Tuple[] values = entry.getValue();

        for( int i = 0; i < functors.length; i++ )
            results[ i + 1 ] = functors[ i ].complete( flowProcess, values[ i ] );

        TupleViews.reset( result, results );

        outputCollector.add( result );
    }

    @Override
    public boolean equals( Object object )
    {
        if( this == object )
            return true;
        if( !( object instanceof CompositeFunctionOrig) )
            return false;
        if( !super.equals( object ) )
            return false;

        CompositeFunctionOrig that = (CompositeFunctionOrig) object;

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