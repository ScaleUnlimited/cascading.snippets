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
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.CountBy;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.InMemoryTap;

public class CountByTest {

    @Test
    public void testUsingValueFields() throws Exception {
        Fields inFields = new Fields("a", "b", "c");
        InMemoryTap sourceTap = new InMemoryTap(inFields);
        TupleEntryCollector writer = sourceTap.openForWrite(new LocalFlowProcess());
        
        writer.add(new Tuple("x", "y", 1));
        writer.add(new Tuple("x", "y", 4));
        writer.add(new Tuple("x", "z", 1));
        writer.add(new Tuple("x", "y", 2));
        writer.add(new Tuple("x", "y", 9));
        writer.close();

        // Now create a flow that does a GroupBy, with a custom buffer
        //
        Pipe pipe = new Pipe("pipe");
        pipe = new CountBy(pipe, new Fields("a", "b"), new Fields("c"), new Fields("count"));
        pipe = new Each(pipe, DebugLevel.VERBOSE, new Debug("aggregated", true));

        // InMemoryTap sinkTap = new InMemoryTap(new Fields("a", "b", "cnt", "ccnt"));
        
        FlowDef flowDef = new FlowDef()
            .addSource(pipe, sourceTap)
            .addTailSink(pipe, new NullSinkTap())
            .setDebugLevel(DebugLevel.VERBOSE);
            
        new LocalFlowConnector().connect(flowDef).complete();
    }

    @Test
    public void testUsingValueFieldsWithAggregateBy() throws Exception {
        Fields inFields = new Fields("a", "b", "c");
        InMemoryTap sourceTap = new InMemoryTap(inFields);
        TupleEntryCollector writer = sourceTap.openForWrite(new LocalFlowProcess());
        
        writer.add(new Tuple("x", "y", 1));
        writer.add(new Tuple("x", "y", 4));
        writer.add(new Tuple("x", "z", 1));
        writer.add(new Tuple("x", "y", 1));
        writer.add(new Tuple("x", "y", 9));
        writer.close();

        // Now create a flow that does a GroupBy, with a custom buffer
        //
        Pipe pipe = new Pipe("pipe");
        AggregateBy countA = new CountBy(new Fields("a"), new Fields("countA"));
        AggregateBy countB = new CountBy(new Fields("b"), new Fields("countB"));
        pipe = new AggregateBy(pipe, Fields.NONE, countA, countB);
        pipe = new Each(pipe, DebugLevel.VERBOSE, new Debug("aggregated", true));

        // InMemoryTap sinkTap = new InMemoryTap(new Fields("a", "b", "cnt", "ccnt"));
        
        FlowDef flowDef = new FlowDef()
            .addSource(pipe, sourceTap)
            .addTailSink(pipe, new NullSinkTap())
            .setDebugLevel(DebugLevel.VERBOSE);
            
        new LocalFlowConnector().connect(flowDef).complete();
    }

}
