package com.scaleunlimited.snippets;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.Debug;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.FirstBy;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.scaleunlimited.cascading.local.DirectoryTap;
import com.scaleunlimited.cascading.local.InMemoryTap;

public class TextDelimitedTest {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testUsingTypesInFields() throws Exception {
        Fields fields = new Fields(new String[]{"id", "value"}, new Class<?>[]{String.class, Integer.class});
        String path = "build/test/TextDelimitedTest/testUsingTypesInFields/";
        Tap sourceTap = new DirectoryTap(new TextDelimited(fields), path, SinkMode.REPLACE);
        
        // Write out some tuples where the value field needs to be processed as an Integer
        // to get the right results out of the FirstBy...if it's a String, then "10" < "1000" < "5",
        // which isn't what we want.
        TupleEntryCollector writer = sourceTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("1", 10));
        writer.add(new Tuple("1", 5));
        writer.add(new Tuple("1", 1000));
        writer.add(new Tuple("2", 2));
        writer.close();
        
        Pipe p = new Pipe("pipe");
        p = new FirstBy(p, new Fields("id"), new Fields("value"));
        p = new Each(p, new Debug(true));
        
        InMemoryTap sinkTap = new InMemoryTap(fields);
        
        FlowDef flowDef = new FlowDef();
        flowDef.addSource(p, sourceTap);
        flowDef.addTailSink(p, sinkTap);
        
        new LocalFlowConnector().connect(flowDef).complete();
        
        TupleEntryIterator iter = sinkTap.openForRead(new LocalFlowProcess());
        assertEquals(new Tuple("1", 5), iter.next().getTuple());
        assertEquals(new Tuple("2", 2), iter.next().getTuple());
    }

}
