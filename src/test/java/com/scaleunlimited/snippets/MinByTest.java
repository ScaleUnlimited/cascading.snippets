package com.scaleunlimited.snippets;

import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.Debug;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.MinBy;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.InMemoryTap;

public class MinByTest {

    @Test
    public void test() throws Exception {
        Tap tap = new InMemoryTap(new Fields(new String[]{"id", "value", "other"}, new Class<?>[]{String.class, Integer.class, String.class}));
        TupleEntryCollector writer = tap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("1", 1000, "Ken"));
        writer.add(new Tuple("1", 100, "Bob"));
        
        // Now force the "1", 1000 entry out of the cache (threshold set to 5 below)
        for (int i = 2; i < 10; i++) {
            writer.add(new Tuple("" + i, i, "Name-" + i));
        }
        
        // Now add another entry with the same key of "1", but with a smaller value. If
        // we don't have sorting, this won't be the first entry in the group.
        writer.add(new Tuple("1", 1, "blah"));
        writer.close();
        
        Pipe p = new Pipe("pipe");
        p = new MinBy(p, new Fields("id"), new Fields("value"), new Fields("value"), 50);
        p = new Each(p, new Debug(true));
        
        FlowDef flowDef = new FlowDef();
        flowDef.addSource(p, tap);
        flowDef.addTailSink(p, new NullSinkTap());
        
        new LocalFlowConnector().connect(flowDef).complete();
    }

}
