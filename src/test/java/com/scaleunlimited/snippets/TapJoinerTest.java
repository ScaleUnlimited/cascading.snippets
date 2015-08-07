package com.scaleunlimited.snippets;

import static org.junit.Assert.*;

import org.junit.Test;

import com.scaleunlimited.cascading.local.InMemoryTap;

import cascading.flow.Flow;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public class TapJoinerTest {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testValid() throws Exception {
        
        Tap tap1 = new InMemoryTap(new Fields("id", "tap1-field"));
        TupleEntryCollector writer1 = tap1.openForWrite(new LocalFlowProcess());
        writer1.add(new Tuple(1, "tap1-field-value1"));
        writer1.add(new Tuple(2, "tap1-field-value2"));
        writer1.close();
        
        Tap tap2 = new InMemoryTap(new Fields("id", "tap2-field"));
        TupleEntryCollector writer2 = tap2.openForWrite(new LocalFlowProcess());
        writer2.add(new Tuple(1, "tap2-field-value1"));
        writer2.add(new Tuple(3, "tap2-field-value2"));
        writer2.close();
        
        Tap destTap = new InMemoryTap(Fields.ALL, Fields.ALL, SinkMode.REPLACE);
        Flow<?> flow = new LocalFlowConnector().connect(TapJoiner.createFlow(tap1, tap2, destTap));
        flow.complete();
        
        TupleEntryIterator iter = destTap.openForRead(new LocalFlowProcess());
        int numTuples = 0;
        while (iter.hasNext()) {
            TupleEntry te = iter.next();

            numTuples += 1;
        }
        iter.close();
        
        assertEquals(3, numTuples);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testInvalid() throws Exception {
        
        Tap tap1 = new InMemoryTap(new Fields("id", "tap1-field"));
        TupleEntryCollector writer1 = tap1.openForWrite(new LocalFlowProcess());
        writer1.add(new Tuple(1, "tap1-field-value1"));
        writer1.add(new Tuple(2, "tap1-field-value2"));
        writer1.close();
        
        Tap tap2 = new InMemoryTap(new Fields("idx", "tap2-field"));
        TupleEntryCollector writer2 = tap2.openForWrite(new LocalFlowProcess());
        writer2.add(new Tuple(1, "tap2-field-value1"));
        writer2.add(new Tuple(3, "tap2-field-value2"));
        writer2.close();
        
        Tap destTap = new InMemoryTap(Fields.ALL, Fields.ALL, SinkMode.REPLACE);
        
        try {
            new LocalFlowConnector().connect(TapJoiner.createFlow(tap1, tap2, destTap));
            fail("Should have thrown exception");
        } catch (Exception e) {
            // should have failed.
        }
    }

}
