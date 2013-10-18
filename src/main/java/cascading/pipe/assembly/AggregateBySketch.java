/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.pipe.assembly;

import java.beans.ConstructorProperties;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.util.TupleViews;
import cascading.pipe.assembly.AggregateBy.Functor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class AggregateBySketch is a {@link SubAssembly} that serves two roles for handling aggregate operations.
 * <p/>
 * The first role is as a base class for composable aggregate operations that have a MapReduce Map side optimization for the
 * Reduce side aggregation. For example 'summing' a value within a grouping can be performed partially Map side and
 * completed Reduce side. Summing is associative and commutative.
 * <p/>
 * AggregateBySketch also supports operations that are not associative/commutative like 'counting'. Counting
 * would result in 'counting' value occurrences Map side but summing those counts Reduce side. (Yes, counting can be
 * transposed to summing Map and Reduce sides by emitting 1's before the first sum, but that's three operations over
 * two, and a hack)
 * <p/>
 * Think of this mechanism as a MapReduce Combiner, but more efficient as no values are serialized,
 * deserialized, saved to disk, and multi-pass sorted in the process, which consume cpu resources in trade of
 * memory and a little or no IO.
 * <p/>
 * Further, Combiners are limited to only associative/commutative operations.
 * <p/>
 * Additionally the Cascading planner can move the Map side optimization
 * to the previous Reduce operation further increasing IO performance (between the preceding Reduce and Map phase which
 * is over HDFS).
 * <p/>
 * The second role of the AggregateBySketch class is to allow for composition of AggregateBySketch
 * sub-classes. That is, {@link SumBy} and {@link CountBy} AggregateBySketch sub-classes can be performed
 * in parallel on the same grouping keys.
 * </p>
 * <p/>
 * AggregateBySketch instances return {@code argumentFields} which are used internally to control the values passed to
 * internal Functor instances. If any argumentFields also have {@link java.util.Comparator}s, they will be used
 * to for secondary sorting (see {@link GroupBy} {@code sortFields}. This feature is used by {@link FirstBy} to
 * control which Tuple is seen first for a grouping.
 * <p/>
 * <p/>
 * Note using a AggregateBySketch instance automatically inserts a {@link GroupBy} into the resulting {@link cascading.flow.Flow}.
 * And passing multiple AggregateBySketch instances to a parent AggregateBySketch instance still results in one GroupBy.
 * <p/>
 * Also note that {@link Unique} is not a CompositeAggregator and is slightly more optimized internally.
 * <p/>
 * Keep in mind the {@link cascading.tuple.Hasher} interface is not honored here (for storing keys in the cache). Thus
 * arrays of primitives and object, like {@code byte[]} will not be properly stored. This is a known issue and will
 * be resolved in a future release.
 *
 * @see SumBy
 * @see CountBy
 * @see Unique
 */
public class AggregateBySketch extends SubAssembly
{
    private static final Logger LOG = LoggerFactory.getLogger( AggregateBySketch.class );



    private String name;
    private int topK;
    private Fields groupingFields;
    private Fields[] argumentFields;
    private AggregateBy.Functor[] functors;
    private Aggregator[] aggregators;
    private transient GroupBy groupBy;





    /**
     * Constructor CompositeAggregator creates a new CompositeAggregator instance.
     *
     * @param name      of type String
     * @param topK of type int
     */
    protected AggregateBySketch(String name, int topK)
    {
        this.name = name;
        this.topK = topK;
    }

    /**
     * Constructor CompositeAggregator creates a new CompositeAggregator instance.
     *
     * @param argumentFields of type Fields
     * @param functor        of type Functor
     * @param aggregator     of type Aggregator
     */
    protected AggregateBySketch(Fields argumentFields, Functor functor, Aggregator aggregator)
    {
        this.argumentFields = Fields.fields( argumentFields );
        this.functors = new Functor[]{functor};
        this.aggregators = new Aggregator[]{aggregator};
    }

    /**
     * Constructor CompositeAggregator creates a new CompositeAggregator instance.
     *
     * @param pipe           of type Pipe
     * @param groupingFields of type Fields
     * @param assemblies     of type CompositeAggregator...
     */
    @ConstructorProperties({"pipe", "groupingFields", "assemblies"})
    public AggregateBySketch(Pipe pipe, Fields groupingFields, AggregateBySketch... assemblies)
    {
        this( null, Pipe.pipes( pipe ), groupingFields, 0, assemblies );
    }

    /**
     * Constructor CompositeAggregator creates a new CompositeAggregator instance.
     *
     * @param pipe           of type Pipe
     * @param groupingFields of type Fields
     * @param topK      of type int
     * @param assemblies     of type CompositeAggregator...
     */
    @ConstructorProperties({"pipe", "groupingFields", "topK", "assemblies"})
    public AggregateBySketch(Pipe pipe, Fields groupingFields, int topK, AggregateBySketch... assemblies)
    {
        this( null, Pipe.pipes( pipe ), groupingFields, topK, assemblies );
    }

    /**
     * Constructor CompositeAggregator creates a new CompositeAggregator instance.
     *
     * @param pipe           of type Pipe
     * @param groupingFields of type Fields
     * @param topK      of type int
     * @param assemblies     of type CompositeAggregator...
     */
    @ConstructorProperties({"name", "pipe", "groupingFields", "topK", "assemblies"})
    public AggregateBySketch(String name, Pipe pipe, Fields groupingFields, int topK, AggregateBySketch... assemblies)
    {
        this( name, Pipe.pipes( pipe ), groupingFields, topK, assemblies );
    }

    /**
     * Constructor CompositeAggregator creates a new CompositeAggregator instance.
     *
     * @param name           of type String
     * @param pipes          of type Pipe[]
     * @param groupingFields of type Fields
     * @param assemblies     of type CompositeAggregator...
     */
    @ConstructorProperties({"name", "pipes", "groupingFields", "assemblies"})
    public AggregateBySketch(String name, Pipe[] pipes, Fields groupingFields, AggregateBySketch... assemblies)
    {
        this( name, pipes, groupingFields, 0, assemblies );
    }

    /**
     * Constructor CompositeAggregator creates a new CompositeAggregator instance.
     *
     * @param name           of type String
     * @param pipes          of type Pipe[]
     * @param groupingFields of type Fields
     * @param topK      of type int
     * @param assemblies     of type CompositeAggregator...
     */
    @ConstructorProperties({"name", "pipes", "groupingFields", "topK", "assemblies"})
    public AggregateBySketch(String name, Pipe[] pipes, Fields groupingFields, int topK, AggregateBySketch... assemblies)
    {
        this( name, topK );

        List<Fields> arguments = new ArrayList<Fields>();
        List<Functor> functors = new ArrayList<Functor>();
        List<Aggregator> aggregators = new ArrayList<Aggregator>();

        for( int i = 0; i < assemblies.length; i++ )
        {
            AggregateBySketch assembly = assemblies[ i ];

            Collections.addAll( arguments, assembly.getArgumentFields() );
            Collections.addAll( functors, assembly.getFunctors() );
            Collections.addAll( aggregators, assembly.getAggregators() );
        }

        initialize( groupingFields, pipes, arguments.toArray( new Fields[ arguments.size() ] ), functors.toArray( new Functor[ functors.size() ] ), aggregators.toArray( new Aggregator[ aggregators.size() ] ) );
    }

    protected AggregateBySketch(String name, Pipe[] pipes, Fields groupingFields, Fields argumentFields, Functor functor, Aggregator aggregator, int topK)
    {
        this( name, topK );
        initialize( groupingFields, pipes, argumentFields, functor, aggregator );
    }

    protected void initialize( Fields groupingFields, Pipe[] pipes, Fields argumentFields, Functor functor, Aggregator aggregator )
    {
        initialize( groupingFields, pipes, Fields.fields( argumentFields ),
                new Functor[]{functor},
                new Aggregator[]{aggregator} );
    }

    protected void initialize( Fields groupingFields, Pipe[] pipes, Fields[] argumentFields, Functor[] functors, Aggregator[] aggregators )
    {
        setPrevious( pipes );

        this.groupingFields = groupingFields;
        this.argumentFields = argumentFields;
        this.functors = functors;
        this.aggregators = aggregators;

        verify();

        Fields sortFields = Fields.copyComparators( Fields.merge( this.argumentFields ), this.argumentFields );
        Fields argumentSelector = Fields.merge( this.groupingFields, sortFields );

        if( argumentSelector.equals( Fields.NONE ) )
            argumentSelector = Fields.ALL;

        Pipe[] functions = new Pipe[ pipes.length ];

        CompositeFunction function = new CompositeFunction( this.groupingFields, this.argumentFields, this.functors, topK);

        for( int i = 0; i < functions.length; i++ )
            functions[ i ] = new Each( pipes[ i ], argumentSelector, function, Fields.RESULTS );

        groupBy = new GroupBy( name, functions, this.groupingFields, sortFields.hasComparators() ? sortFields : null );

        Pipe pipe = groupBy;

        for( int i = 0; i < aggregators.length; i++ )
            pipe = new Every( pipe, this.functors[ i ].getDeclaredFields(), this.aggregators[ i ], Fields.ALL );

        setTails( pipe );
    }

    /** Method verify should be overridden by sub-classes if any values must be tested before the calling constructor returns. */
    protected void verify()
    {

    }

    /**
     * Method getGroupingFields returns the Fields this instances will be grouping against.
     *
     * @return the current grouping fields
     */
    public Fields getGroupingFields()
    {
        return groupingFields;
    }

    /**
     * Method getFieldDeclarations returns an array of Fields where each Field element in the array corresponds to the
     * field declaration of the given Aggregator operations.
     * <p/>
     * Note the actual Fields values are returned, not planner resolved Fields.
     *
     * @return and array of Fields
     */
    public Fields[] getFieldDeclarations()
    {
        Fields[] fields = new Fields[ this.aggregators.length ];

        for( int i = 0; i < aggregators.length; i++ )
            fields[ i ] = aggregators[ i ].getFieldDeclaration();

        return fields;
    }

    protected Fields[] getArgumentFields()
    {
        return argumentFields;
    }

    protected Functor[] getFunctors()
    {
        return functors;
    }

    protected Aggregator[] getAggregators()
    {
        return aggregators;
    }

    /**
     * Method getGroupBy returns the internal {@link GroupBy} instance so that any custom properties
     * can be set on it via {@link cascading.pipe.Pipe#getStepConfigDef()}.
     *
     * @return GroupBy type
     */
    public GroupBy getGroupBy()
    {
        return groupBy;
    }
}
