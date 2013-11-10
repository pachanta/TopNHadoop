/**
 */
package com.etleap.cascading.topn;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Function;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.Sum;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.CountBySketch;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;


import java.util.Properties;

/**
 * Code for SpaceSavingTopN
 */
public class TopNCascading {

    @SuppressWarnings("rawtypes")
    public final static void main(String[] args) {

        String inputPath = args[0];
        String outputPath = args[1];
        String topK       = args[2];

        // Define source and sink Taps.


        Scheme sourceScheme = new TextLine(new Fields("line"));
        Tap source = new Hfs(sourceScheme, inputPath);

        Scheme sinkScheme = new TextLine(new Fields("word", "count"));
        Tap sink = new Hfs(sinkScheme, outputPath, SinkMode.REPLACE);

        // the 'head' of the pipe assembly
        Pipe assembly = new Pipe("wordcount");

        // For each input Tuple
        // parse out each word into a new Tuple with the field name "word"
        // regular expressions are optional in Cascading
        Function function = new RegexSplitGenerator(new Fields("word"), "\\s+");
        assembly = new Each(assembly, new Fields("line"), function);

        // For every Tuple group
        // count the number of occurrences of "word" and store result in
        // a field named "count"
        assembly = new Each(assembly, new Fields("word"), new TopNFunction(new Fields("word","count"),Integer.valueOf(topK)));

        assembly = new GroupBy( "topn", assembly,new Fields("word"));
        assembly = new Every(assembly,new Fields("count"), new Sum(),Fields.ALL);
        assembly = new Each(assembly, new Fields("word"), new TopNFunction(new Fields("word","count"),Integer.valueOf(topK)));
        // initialize app properties, tell Hadoop which jar file to use
        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, TopNCascading.class);

        // plan a new Flow from the assembly using the source and sink Taps
        // with the above properties

        // connect the taps, pipes, etc., into a flow
        FlowDef flowDef = FlowDef.flowDef()
                .setName( "wc" )
                .addSource( assembly, source )
                .addTailSink( assembly, sink );


        HadoopFlowConnector flowConnector = new HadoopFlowConnector( properties );
        //Flow flow = flowConnector.connect("word-count", source, sink, assembly);
        Flow flow = flowConnector.connect(flowDef);


        // execute the flow, block until complete
        flow.complete();


    }
}
