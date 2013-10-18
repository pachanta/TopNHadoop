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

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.aggregator.Sum;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.Tuples;
import cascading.pipe.assembly.AggregateBy.Functor;

/**
 * Class CountBySketch is used to count duplicates in a tuple stream.
 * <p/>
 * Typically finding the count of a field in a tuple stream relies on a {@link cascading.pipe.GroupBy} and a {@link cascading.operation.aggregator.Count}
 * {@link cascading.operation.Aggregator} operation.
 * <p/>
 * If {@code include} is {@link Include#NO_NULLS}, argument tuples with all null values will be ignored. When counting
 * the occurrence of a single field (when {@code valueFields} is set on the constructor), this is the same behavior
 * as {@code select count(foo) ...} in SQL. If {@code include} is
 * {@link Include#ONLY_NULLS} then only tuples will all null values will be counted.
 * <p/>
 * This SubAssembly also uses the {@link CountBySketch.CountPartials} {@link AggregateBy.Functor}
 * to count field values before the GroupBy operator to reduce IO over the network.
 * <p/>
 * This strategy is similar to using {@code combiners}, except no sorting or serialization is invoked and results
 * in a much simpler mechanism.
 * <p/>
 * The {@code topK} value tells the underlying CountPartials functions how many unique key counts to accumulate
 * in the LRU cache, before emitting the least recently used entry.
 * <p/>
 *
 * @see AggregateBySketch
 */
public class CountBySketch extends AggregateBySketch
{
    

    public enum Include
    {
        ALL,
        NO_NULLS,
        ONLY_NULLS
    }

    /**
     * Class CountPartials is a {@link AggregateBy.Functor} that is used to count observed duplicates from the tuple stream.
     * <p/>
     * Use this class typically in tandem with a {@link cascading.operation.aggregator.Sum}
     * {@link cascading.operation.Aggregator} in order to improve counting performance by removing as many values
     * as possible before the intermediate {@link cascading.pipe.GroupBy} operator.
     *
     * @see CountBySketch
     */
    public static class CountPartials implements Functor
    {
        private final Fields declaredFields;
        private final Include include;

        /**
         * Constructor CountPartials creates a new CountPartials instance.
         *
         * @param declaredFields of type Fields
         */
        public CountPartials( Fields declaredFields )
        {
            this( declaredFields, Include.ALL );
        }

        public CountPartials( Fields declaredFields, Include include )
        {
            this.declaredFields = declaredFields;
            this.include = include;

            if( !declaredFields.isDeclarator() || declaredFields.size() != 1 )
                throw new IllegalArgumentException( "declaredFields should declare only one field name" );
        }

        @Override
        public Fields getDeclaredFields()
        {
            return declaredFields;
        }

        @Override
        public Tuple aggregate( FlowProcess flowProcess, TupleEntry args, Tuple context )
        {
            if( context == null )
                context = new Tuple( 0L );

            switch( include )
            {
                case ALL:
                    break;

                case NO_NULLS:
                    if( Tuples.frequency( args, null ) == args.size() )
                        return context;

                    break;

                case ONLY_NULLS:
                    if( Tuples.frequency( args, null ) != args.size() )
                        return context;

                    break;
            }

            context.set( 0, context.getLong( 0 ) + 1L );

            return context;
        }

        @Override
        public Tuple complete( FlowProcess flowProcess, Tuple context )
        {
            return context;
        }
    }







    //// AggregateBySketch param constructors

    /**
     * Constructor CountBySketch creates a new CountBySketch instance. Use this constructor when used with a {@link AggregateBySketch}
     * instance.
     *
     * @param countField of type Fields
     */
    @ConstructorProperties({"countField"})
    public CountBySketch(Fields countField)
    {
        super( Fields.ALL, new CountPartials( countField.applyTypes( Long.TYPE ) ), new Sum( countField.applyTypes( Long.TYPE ) ) );
    }

    /**
     * Constructor CountBySketch creates a new CountBySketch instance. Use this constructor when used with a {@link AggregateBySketch}
     * instance.
     *
     * @param countField of type Fields
     * @param include    of type Include
     */
    @ConstructorProperties({"countField", "include"})
    public CountBySketch(Fields countField, Include include)
    {
        super( Fields.ALL, new CountPartials( countField.applyTypes( Long.TYPE ), include ), new Sum( countField.applyTypes( Long.TYPE ) ) );
    }

    /**
     * Constructor CountBySketch creates a new CountBySketch instance. Use this constructor when used with a {@link AggregateBySketch}
     * instance.
     *
     * @param countField of type Fields
     */
    @ConstructorProperties({"valueFields", "countField"})
    public CountBySketch(Fields valueFields, Fields countField)
    {
        super( valueFields, new CountPartials( countField.applyTypes( Long.TYPE ) ), new Sum( countField.applyTypes( Long.TYPE ) ) );
    }

    /**
     * Constructor CountBySketch creates a new CountBySketch instance. Use this constructor when used with a {@link AggregateBySketch}
     * instance.
     *
     * @param countField of type Fields
     */
    @ConstructorProperties({"valueFields", "countField", "include"})
    public CountBySketch(Fields valueFields, Fields countField, Include include)
    {
        super( valueFields, new CountPartials( countField.applyTypes( Long.TYPE ), include ), new Sum( countField.applyTypes( Long.TYPE ) ) );
    }


    /**
     * Constructor CountBySketch creates a new CountBySketch instance.
     *
     * @param pipe           of type Pipe
     * @param groupingFields of type Fields
     * @param countField     fo type Fields
     * @param topK      of type int
     */
    @ConstructorProperties({"pipe", "groupingFields", "countField", "topK"})
    public CountBySketch(Pipe pipe, Fields groupingFields, Fields countField, int topK)
    {
        this( null, pipe, groupingFields, countField, topK );
    }


    /**
     * Constructor CountBySketch creates a new CountBySketch instance.
     *
     * @param name           of type String
     * @param pipe           of type Pipe
     * @param groupingFields of type Fields
     * @param countField     of type Fields
     * @param topK      of type int
     */
    @ConstructorProperties({"name", "pipe", "groupingFields", "countField", "topK"})
    public CountBySketch(String name, Pipe pipe, Fields groupingFields, Fields countField, int topK)
    {
        this( name, Pipe.pipes( pipe ), groupingFields, countField, topK );
    }


    /**
     * Constructor CountBySketch creates a new CountBySketch instance.
     *
     * @param pipes          of type Pipe[]
     * @param groupingFields of type Fields
     * @param countField     of type Fields
     * @param topK      of type int
     */
    @ConstructorProperties({"pipes", "groupingFields", "countField", "topK"})
    public CountBySketch(Pipe[] pipes, Fields groupingFields, Fields countField, int topK)
    {
        this( null, pipes, groupingFields, countField, topK );
    }



    /**
     * Constructor CountBySketch creates a new CountBySketch instance.
     *
     * @param name           of type String
     * @param pipes          of type Pipe[]
     * @param groupingFields of type Fields
     * @param countField     of type Fields
     * @param topK      of type int
     */
    @ConstructorProperties({"name", "pipes", "groupingFields", "countField", "topK"})
    public CountBySketch(String name, Pipe[] pipes, Fields groupingFields, Fields countField, int topK)
    {
        super( name, pipes, groupingFields, groupingFields, new CountPartials( countField.applyTypes( Long.TYPE ) ), new Sum( countField.applyTypes( Long.TYPE ) ), topK );
    }



    /**
     * Constructor CountBySketch creates a new CountBySketch instance.
     *
     * @param pipe           of type Pipe
     * @param groupingFields of type Fields
     * @param countField     fo type Fields
     * @param include        of type Include
     * @param topK      of type int
     */
    @ConstructorProperties({"pipe", "groupingFields", "countField", "include", "topK"})
    public CountBySketch(Pipe pipe, Fields groupingFields, Fields countField, Include include, int topK)
    {
        this( null, pipe, groupingFields, countField, include, topK );
    }



    /**
     * Constructor CountBySketch creates a new CountBySketch instance.
     *
     * @param name           of type String
     * @param pipe           of type Pipe
     * @param groupingFields of type Fields
     * @param countField     of type Fields
     * @param include        of type Include
     * @param topK      of type int
     */
    @ConstructorProperties({"name", "pipe", "groupingFields", "countField", "include", "topK"})
    public CountBySketch(String name, Pipe pipe, Fields groupingFields, Fields countField, Include include, int topK)
    {
        this( name, Pipe.pipes( pipe ), groupingFields, countField, include, topK );
    }



    /**
     * Constructor CountBySketch creates a new CountBySketch instance.
     *
     * @param pipes          of type Pipe[]
     * @param groupingFields of type Fields
     * @param countField     of type Fields
     * @param include        of type Include
     * @param topK      of type int
     */
    @ConstructorProperties({"pipes", "groupingFields", "countField", "include", "topK"})
    public CountBySketch(Pipe[] pipes, Fields groupingFields, Fields countField, Include include, int topK)
    {
        this( null, pipes, groupingFields, countField, include, topK );
    }



    /**
     * Constructor CountBySketch creates a new CountBySketch instance.
     *
     * @param name           of type String
     * @param pipes          of type Pipe[]
     * @param groupingFields of type Fields
     * @param countField     of type Fields
     * @param include        of type Include
     * @param topK      of type int
     */
    @ConstructorProperties({"name", "pipes", "groupingFields", "countField", "include", "topK"})
    public CountBySketch(String name, Pipe[] pipes, Fields groupingFields, Fields countField, Include include, int topK)
    {
        super( name, pipes, groupingFields, groupingFields, new CountPartials( countField.applyTypes( Long.TYPE ), include ), new Sum( countField.applyTypes( Long.TYPE ) ), topK );
    }


    /**
     * Constructor CountBySketch creates a new CountBySketch instance.
     *
     * @param pipe           of type Pipe
     * @param groupingFields of type Fields
     * @param valueFields    of type Fields
     * @param countField     fo type Fields
     * @param topK      of type int
     */
    @ConstructorProperties({"pipe", "groupingFields", "valueFields", "countField", "topK"})
    public CountBySketch(Pipe pipe, Fields groupingFields, Fields valueFields, Fields countField, int topK)
    {
        this( null, pipe, groupingFields, valueFields, countField, topK );
    }



    /**
     * Constructor CountBySketch creates a new CountBySketch instance.
     *
     * @param name           of type String
     * @param pipe           of type Pipe
     * @param groupingFields of type Fields
     * @param valueFields    of type Fields
     * @param countField     of type Fields
     * @param topK      of type int
     */
    @ConstructorProperties({"name", "pipe", "groupingFields", "valueFields", "countField", "topK"})
    public CountBySketch(String name, Pipe pipe, Fields groupingFields, Fields valueFields, Fields countField, int topK)
    {
        this( name, Pipe.pipes( pipe ), groupingFields, valueFields, countField, topK );
    }


    /**
     * Constructor CountBySketch creates a new CountBySketch instance.
     *
     * @param pipes          of type Pipe[]
     * @param groupingFields of type Fields
     * @param valueFields    of type Fields
     * @param countField     of type Fields
     * @param topK      of type int
     */
    @ConstructorProperties({"pipes", "groupingFields", "valueFields", "countField", "topK"})
    public CountBySketch(Pipe[] pipes, Fields groupingFields, Fields valueFields, Fields countField, int topK)
    {
        this( null, pipes, groupingFields, valueFields, countField, topK );
    }



    /**
     * Constructor CountBySketch creates a new CountBySketch instance.
     *
     * @param name           of type String
     * @param pipes          of type Pipe[]
     * @param groupingFields of type Fields
     * @param valueFields    of type Fields
     * @param countField     of type Fields
     * @param topK      of type int
     */
    @ConstructorProperties({"name", "pipes", "groupingFields", "valueFields", "countField", "topK"})
    public CountBySketch(String name, Pipe[] pipes, Fields groupingFields, Fields valueFields, Fields countField, int topK)
    {
        super( name, pipes, groupingFields, valueFields, new CountPartials( countField.applyTypes( Long.TYPE ) ), new Sum( countField.applyTypes( Long.TYPE ) ), topK );
    }


    /**
     * Constructor CountBySketch creates a new CountBySketch instance.
     *
     * @param pipe           of type Pipe
     * @param groupingFields of type Fields
     * @param valueFields    of type Fields
     * @param countField     fo type Fields
     * @param include        of type Include
     * @param topK      of type int
     */
    @ConstructorProperties({"pipe", "groupingFields", "valueFields", "countField", "include", "topK"})
    public CountBySketch(Pipe pipe, Fields groupingFields, Fields valueFields, Fields countField, Include include, int topK)
    {
        this( null, pipe, groupingFields, valueFields, countField, include, topK );
    }


    /**
     * Constructor CountBySketch creates a new CountBySketch instance.
     *
     * @param name           of type String
     * @param pipe           of type Pipe
     * @param groupingFields of type Fields
     * @param valueFields    of type Fields
     * @param countField     of type Fields
     * @param include        of type Include
     * @param topK      of type int
     */
    @ConstructorProperties({"name", "pipe", "groupingFields", "valueFields", "countField", "include", "topK"})
    public CountBySketch(String name, Pipe pipe, Fields groupingFields, Fields valueFields, Fields countField, Include include, int topK)
    {
        this( name, Pipe.pipes( pipe ), groupingFields, valueFields, countField, include, topK );
    }



    /**
     * Constructor CountBySketch creates a new CountBySketch instance.
     *
     * @param pipes          of type Pipe[]
     * @param groupingFields of type Fields
     * @param valueFields    of type Fields
     * @param countField     of type Fields
     * @param include        of type Include
     * @param topK      of type int
     */
    @ConstructorProperties({"pipes", "groupingFields", "valueFields", "countField", "include", "topK"})
    public CountBySketch(Pipe[] pipes, Fields groupingFields, Fields valueFields, Fields countField, Include include, int topK)
    {
        this( null, pipes, groupingFields, valueFields, countField, include, topK );
    }



    /**
     * Constructor CountBySketch creates a new CountBySketch instance.
     *
     * @param name           of type String
     * @param pipes          of type Pipe[]
     * @param groupingFields of type Fields
     * @param valueFields    of type Fields
     * @param countField     of type Fields
     * @param include        of type Include
     * @param topK      of type int
     */
    @ConstructorProperties({"name", "pipes", "groupingFields", "valueFields", "countField", "include", "topK"})
    public CountBySketch(String name, Pipe[] pipes, Fields groupingFields, Fields valueFields, Fields countField, Include include, int topK)
    {
        super( name, pipes, groupingFields, valueFields, new CountPartials( countField.applyTypes( Long.TYPE ), include ), new Sum( countField.applyTypes( Long.TYPE ) ), topK );
    }
}
