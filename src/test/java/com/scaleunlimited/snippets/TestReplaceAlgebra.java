package com.scaleunlimited.snippets;

import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.Debug;
import cascading.operation.text.DateParser;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.InMemoryTap;

public class TestReplaceAlgebra {

    @Test
    public void test() throws Exception {
        Tap tap = new InMemoryTap(new Fields("BUYER_ID", "CREATED_DT"));
        TupleEntryCollector writer = tap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple(1, "2013-01-01"));
        writer.add(new Tuple(1, "2014-01-01"));
        writer.add(new Tuple(2, "2013-02-01"));
        writer.close();
        
        Pipe p = new Pipe("pipe");
        Pipe results = new Each(p, new Fields("CREATED_DT"), new DateParser(new Fields("CREATED_DT"), "yyyy-MM-dd"), Fields.REPLACE);
        results = new Each(results, new Debug(true));
        
        FlowDef flowDef = new FlowDef();
        flowDef.addSource(p, tap);
        flowDef.addTailSink(results, new NullSinkTap());
        
        new LocalFlowConnector().connect(flowDef).complete();
    }


}
