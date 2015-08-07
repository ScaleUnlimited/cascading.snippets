package com.scaleunlimited.snippets;

import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.local.InMemoryTap;

public class CountAndUniqueCountBufferTest {

    @Test
    public void testBufferDoingCountAndUniqueCount() throws Exception {
        Fields inFields = new Fields("a", "b", "c", "d");
        InMemoryTap sourceTap = new InMemoryTap(inFields);
        TupleEntryCollector writer = sourceTap.openForWrite(new LocalFlowProcess());
        
        writer.add(new Tuple("x", "y", 1, "junk"));
        writer.add(new Tuple("x", "y", 1, "whatever"));
        writer.add(new Tuple("x", "z", 1, "stuff"));
        writer.add(new Tuple("x", "y", 2, "anywho"));
        writer.add(new Tuple("x", "y", 2, "yup"));
        writer.close();

        // Now create a flow that does a GroupBy, with a custom buffer
        //
        Pipe pipe = new Pipe("custom buffer");
        pipe = new GroupBy( pipe, new Fields("a", "b"), new Fields("c"));
        pipe = new Every(pipe,
                         new Fields("c"),
                         new CountAndUniqueCountBuffer("cnt", "ccnt"), 
                         Fields.SWAP);
        pipe = new Each(pipe, DebugLevel.VERBOSE, new Debug("aggregated", true));

        InMemoryTap sinkTap = new InMemoryTap(new Fields("a", "b", "cnt", "ccnt"));
        
        FlowDef flowDef = new FlowDef()
            .addSource(pipe, sourceTap)
            .addTailSink(pipe, sinkTap)
            .setDebugLevel(DebugLevel.VERBOSE);
            
        FlowConnector flowConnector = new LocalFlowConnector();
        Flow flow = flowConnector.connect(flowDef);
        flow.complete();

    }

}
