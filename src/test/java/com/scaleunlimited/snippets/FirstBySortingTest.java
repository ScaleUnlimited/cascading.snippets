package com.scaleunlimited.snippets;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.Debug;
import cascading.operation.Identity;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.FirstBy;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.DirectoryTap;
import com.scaleunlimited.cascading.local.InMemoryTap;
import com.scaleunlimited.cascading.local.KryoScheme;

public class FirstBySortingTest {

    @SuppressWarnings("serial")
    private static class IntegerComparator implements Comparator<Integer>, Serializable {

        @Override
        public int compare(Integer o1, Integer o2) {
            return o1 - o2;
        }
    }
    
    @Test
    public void testSortingWithComparator() throws Exception {
        
        // This test will fail (as expected) with threshold 5 and setComparator false.
        
        // If you bump the threshold to 50, then this test still works even with
        // setComparator set to false, because the map-side FirstPartial does
        // comparisons when deciding whether to update the cache.
        final int threshold = 50;
        final boolean setComparator = false;
        
        Fields fields = new Fields(new String[]{"id", "value"}, new Class<?>[]{String.class, Integer.class});
        Tap tap = new InMemoryTap(fields);
        TupleEntryCollector writer = tap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("1", 1000));
        
        // Now force the ("1", 1000) entry out of the cache (threshold set to 5 below)
        for (int i = 2; i < 10; i++) {
            writer.add(new Tuple("" + i, i));
        }
        
        // Now add another entry with the same key of "1", but with a smaller value. If
        // we don't have sorting, this won't be the first entry in the group.
        writer.add(new Tuple("1", 1));
        writer.close();
        
        Pipe p = new Pipe("pipe");
        Fields sortingField = new Fields("value");
        if (setComparator) {
            sortingField.setComparator("value", new IntegerComparator());
        }
        
        p = new FirstBy(p, new Fields("id"), sortingField, threshold);
        p = new Each(p, new Debug(true));
        
        Tap sinkTap = new InMemoryTap(fields);
        FlowDef flowDef = new FlowDef();
        flowDef.addSource(p, tap);
        flowDef.addTailSink(p, sinkTap);
        
        new LocalFlowConnector().connect(flowDef).complete();
        
        // Verify the entry for key "1" has value 1
        TupleEntryIterator iter = sinkTap.openForRead(new LocalFlowProcess());
        boolean foundTarget = false;
        while (iter.hasNext()) {
            TupleEntry te = iter.next();
            if (te.getString("id").equals("1")) {
                assertEquals(1, te.getInteger("value"));
                foundTarget = true;
                break;
            }
        }
        
        assertTrue(foundTarget);
    }

    @Test
    public void mergeAndSort() throws Exception {
        final Fields sourceFields = new Fields("f1", "f2", "f3", "f4");
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
        Pipe p3 = new FirstBy(Pipe.pipes(p1, p2), new Fields("f1", "f2"), new Fields("f4"));
        p3 = new Each(p3, new Debug(true));
        
        FlowDef flowDef = new FlowDef()
            .addSource(p1, tap1)
            .addSource(p2, tap2)
            .addTailSink(p3, new NullSinkTap());
        
        new LocalFlowConnector().connect(flowDef).complete();
    }


}
