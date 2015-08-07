package com.scaleunlimited.snippets;

import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.Debug;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.filter.And;
import cascading.operation.filter.Or;
import cascading.operation.regex.RegexFilter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.InMemoryTap;

public class FilterTest {

    @Test
    public void testAndOrFiltering() throws Exception {
        Fields fields = new Fields("state", "city", "name", "value");
        InMemoryTap inTap = new InMemoryTap(fields);
        TupleEntryCollector writer = inTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("CA", "Nevada City", "Rob", 7));
        writer.add(new Tuple("WA", "Nevada City", "Rob", 5));
        writer.add(new Tuple("CA", "Auburn", "Rob", 10));
        writer.add(new Tuple("CA", "Nevada City", "Robert", 1));
        writer.add(new Tuple("CA", "Nevada City", "Tom", 3));
        writer.close();

        Pipe filterPipe = new Pipe("p");
        filterPipe = new Each(filterPipe, new Debug("before", true));
        ExpressionFilter stateFilter = new ExpressionFilter("!$0.equals(\"CA\")", String.class);
        ExpressionFilter cityFilter = new ExpressionFilter("!$1.equals(\"Nevada City\")", String.class);
        
        // Create a pattern that matches anything (not greedy) in the first two fields, and then the third field has
        // to start with "Rob"
        RegexFilter nameFilter = new RegexFilter("(.*?)\\t(.*?)\\tRob.*");
        filterPipe = new Each(filterPipe, new Fields("state", "city", "name"), new Or(stateFilter, cityFilter, nameFilter));
        filterPipe = new Each(filterPipe, new Debug("after", true));

        // Start the Flow
        FlowDef flowDef = FlowDef.flowDef()
                .addSource(filterPipe, inTap)
                .addTailSink(filterPipe, new NullSinkTap());
        new LocalFlowConnector().connect( flowDef ).complete(); 
    }
    
    @Test
    public void testMultiFieldRegex() throws Exception {
        Fields fields = new Fields("state", "city", "name", "value");
        InMemoryTap inTap = new InMemoryTap(fields);
        TupleEntryCollector writer = inTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("CA", "Nevada City", "Rob", 7));
        writer.add(new Tuple("WA", "Nevada City", "Rob", 5));
        writer.add(new Tuple("CA", "Auburn", "Rob", 10));
        writer.add(new Tuple("CA", "Nevada City", "Robert", 1));
        writer.add(new Tuple("CA", "Nevada City", "Tom", 3));
        writer.close();

        Pipe filterPipe = new Pipe("p");
        filterPipe = new Each(filterPipe, new Debug("before", true));
        
        // Create a pattern that matches anything (not greedy) in the first two fields, and then the third field has
        // to start with "Rob"
        RegexFilter nameFilter = new RegexFilter("CA\\tNevada City\\tRob.*", false);
        filterPipe = new Each(filterPipe, new Fields("state", "city", "name"), nameFilter);
        filterPipe = new Each(filterPipe, new Debug("after", true));

        // Start the Flow
        FlowDef flowDef = FlowDef.flowDef()
                .addSource(filterPipe, inTap)
                .addTailSink(filterPipe, new NullSinkTap());
        new LocalFlowConnector().connect( flowDef ).complete(); 
    }
    
    
}
