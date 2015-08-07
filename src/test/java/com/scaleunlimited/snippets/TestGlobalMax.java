package com.scaleunlimited.snippets;

import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.Debug;
import cascading.operation.Identity;
import cascading.operation.aggregator.First;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.InMemoryTap;

public class TestGlobalMax {

    @Test
    public void test() throws Exception {
        Tap tap = new InMemoryTap(new Fields("id", "value"));
        TupleEntryCollector writer = tap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("id1", 1));
        writer.add(new Tuple("id2", 5));
        writer.add(new Tuple("id3", -10));
        writer.add(new Tuple("id4", 100));
        writer.close();
        
        Pipe p = new Pipe("pipe");
        p = new Each(p, new Fields("value"), new Identity());
        p = new GroupBy(p, Fields.NONE, new Fields("value"), true);
        p = new Every(p, new First());
        p = new Each(p, new Debug(true));
        
        FlowDef flowDef = new FlowDef();
        flowDef.addSource(p, tap);
        flowDef.addTailSink(p, new NullSinkTap());
        
        new LocalFlowConnector().connect(flowDef).complete();
    }


}
