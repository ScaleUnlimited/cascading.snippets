package com.scaleunlimited.snippets;

import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.Debug;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.MaxBy;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.InMemoryTap;

public class MaxByTest {

    @Test
    public void test() throws Exception {
        Tap tap = new InMemoryTap(new Fields(new String[]{"id", "flag", "other"}, new Class<?>[]{String.class, Boolean.class, String.class}));
        TupleEntryCollector writer = tap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("1", false, "Ken"));
        writer.add(new Tuple("1", true, "Bob"));
        writer.close();
        
        Pipe p = new Pipe("pipe");
        p = new MaxBy(p, new Fields("id"), new Fields("flag"), new Fields("flag"));
        p = new Each(p, new Debug(true));
        
        FlowDef flowDef = new FlowDef();
        flowDef.addSource(p, tap);
        flowDef.addTailSink(p, new NullSinkTap());
        
        new LocalFlowConnector().connect(flowDef).complete();
    }

}
