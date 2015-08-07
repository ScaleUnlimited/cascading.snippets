package com.scaleunlimited.snippets;

import static org.junit.Assert.*;

import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.Debug;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.FirstBy;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.InMemoryTap;

public class SameByTest {

    @Test
    public void test() throws Exception {
        final Fields sourceFields = new Fields("F1", "F2", "F3", "F4");
        Tap tap1 = new InMemoryTap(sourceFields);
        TupleEntryCollector writer1 = tap1.openForWrite(new LocalFlowProcess());
        writer1.add(new Tuple(1, 2, 3, "20141010"));
        writer1.add(new Tuple(1, 2, 4, "20141010"));
        writer1.close();
        
        Tap tap2 = new InMemoryTap(sourceFields);
        TupleEntryCollector writer2 = tap2.openForWrite(new LocalFlowProcess());
        writer2.add(new Tuple(1, 2, 5, "20141009"));
        writer2.add(new Tuple(1, 2, 6, "20141009"));
        writer2.close();
        
        Pipe p1 = new Pipe("pipe one");
        Pipe p2 = new Pipe("pipe two");
        Pipe p3 = new SameBy(Pipe.pipes(p1, p2), new Fields("F1", "F2"), new Fields("F4"), true);
        p3 = new Each(p3, new Debug(true));
        
        FlowDef flowDef = new FlowDef()
            .addSource(p1, tap1)
            .addSource(p2, tap2)
            .addTailSink(p3, new NullSinkTap());
        
        new LocalFlowConnector().connect(flowDef).complete();
    }

}
