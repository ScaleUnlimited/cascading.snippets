package com.scaleunlimited.snippets;

import org.apache.log4j.Logger;
import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.Debug;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.InMemoryTap;

public class ThreeFieldJoinTest {

    @Test
    public void testThreeFieldJoin() throws Exception {
        InMemoryTap lhsTap = new InMemoryTap(ThreeFieldJoin.LHS_FIELDS);
        TupleEntryCollector writer = lhsTap.openForWrite(new LocalFlowProcess());
        
        writer.add(new Tuple("asf", 34567));
        writer.add(new Tuple("sdf", 65478));
        writer.add(new Tuple("ght", 12679));
        writer.close();

        InMemoryTap rhsTap = new InMemoryTap(ThreeFieldJoin.RHS_FIELDS);
        writer = rhsTap.openForWrite(new LocalFlowProcess());
        
        writer.add(new Tuple("hht", "asf", "ght"));
        writer.add(new Tuple("ght", "asf", "sdf"));
        writer.add(new Tuple("asf", "xxx", "yyy"));
        writer.close();

        Pipe lhsPipe = new Pipe("lhs");
        Pipe rhsPipe = new Pipe("rhs");
        Pipe results = new ThreeFieldJoin(lhsPipe, rhsPipe);
        results = new Each(results, new Debug(true));
        
        FlowDef flowDef = new FlowDef();
        flowDef.addSource(lhsPipe, lhsTap);
        flowDef.addSource(rhsPipe, rhsTap);
        flowDef.addTailSink(results, new NullSinkTap());
        
        new LocalFlowConnector().connect(flowDef).complete();
    }

}
