package com.scaleunlimited.snippets;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.DebugLevel;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Rename;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.scaleunlimited.cascading.local.InMemoryTap;

public class MarkovModelSubAssemblyTest {

    @Test
    public void testSimpleCase() throws Exception {
        Tuple[] inputData = new Tuple[] {
                        new Tuple("a", "1"),
                        new Tuple("a", "1"),
                        new Tuple("a", "1"),
                        new Tuple("a", "2"),
                        new Tuple("b", "10")
        };
        
        Tuple[] results = new Tuple[] {
                        new Tuple("a", "1", 0.75f),
                        new Tuple("a", "2", 0.25f),
                        new Tuple("b", "10", 1.0f)
        };
        
        runTest(inputData, results);
    }
    
    @Test
    public void testLimits() throws Exception {
        Tuple[] inputData = new Tuple[] {
                        new Tuple("a", "1"),
                        new Tuple("a", "1"),
                        new Tuple("a", "1"),
                        new Tuple("a", "1"),
                        new Tuple("a", "2"),
                        new Tuple("a", "2"),
                        new Tuple("a", "2"),
                        new Tuple("a", "3"),
                        new Tuple("b", "10")
        };
        
        // Verify with two leading values, but still only one result per leading value
        Tuple[] results2 = new Tuple[] {
                        new Tuple("a", "1", 0.5f),
                        new Tuple("b", "10", 1.0f)
        };
        runTest(inputData, results2, new Fields("result"), 2, 1);
        
        // Verify with only one leading value, and one result per leading value
        Tuple[] results1 = new Tuple[] {
                        new Tuple("a", "1", 0.50f),
        };
        runTest(inputData, results1, new Fields("result"), 1, 1);
        
        // Verify with one leading value, and two results per leading value
        Tuple[] results3 = new Tuple[] {
                        new Tuple("a", "1", 0.50f),
                        new Tuple("a", "2", 0.375f),
        };
        runTest(inputData, results3, new Fields("result"), 1, 2);
        
        
    }
    
    protected void runTest(Tuple[] inputTuples, Tuple[] results) throws Exception {
        runTest(inputTuples, results, new Fields(MarkovModelSubAssembly.BIGRAM_PROBABILITY_FN), Integer.MAX_VALUE, Integer.MAX_VALUE);
    }
    
    protected void runTest(Tuple[] inputTuples, Tuple[] results, Fields resultField, int maxLeadingValues, int maxResultsPerLeadingValue) throws Exception {
        final Fields sourceFields = new Fields("leading", "trailing");
        InMemoryTap sourceTap = new InMemoryTap(sourceFields);
        TupleEntryCollector writer = sourceTap.openForWrite(new LocalFlowProcess());
        for (Tuple inputTuple : inputTuples) {
            writer.add(inputTuple);
        }
        writer.close();
        
        Pipe p = new Pipe("source");
        p = new MarkovModelSubAssembly(p, resultField, maxLeadingValues, maxResultsPerLeadingValue);
        
        // Note that the SubAssembly has lost our field names, so if we want to use
        // field names we have to rename things.
        p = new Rename(p, new Fields(0, 1), sourceFields);
        
        InMemoryTap sinkTap = new InMemoryTap(  Fields.ALL,
                                                sourceFields.append(resultField),
                                                SinkMode.REPLACE);
        
        FlowDef flowDef = new FlowDef()
            .setName("MarkovModelSubAssemblyTest-test")
            .addSource(p, sourceTap)
            .addTailSink(p, sinkTap)
            .setDebugLevel(DebugLevel.VERBOSE);
        
        new LocalFlowConnector().connect(flowDef).complete();
        
        // Now verify we have the expected results in our result.
        assertContains(sinkTap.openForRead(new LocalFlowProcess()), results);
    }
    
    private static void assertContains(TupleEntryIterator iter, Tuple... values) {
        List<Tuple> targets = new ArrayList<Tuple>(Arrays.asList(values));
        
        while (iter.hasNext()) {
            Tuple t = iter.next().getTuple();
            boolean foundValue = false;
            
            Iterator<Tuple> targetIter = targets.iterator();
            while (targetIter.hasNext() && !foundValue) {
                Tuple targetTuple = targetIter.next();
                if (targetTuple.size() != t.size()) {
                    continue;
                }
                
                boolean matches = true;
                for (int i = 0; i < t.size() && matches; i++) {
                    matches = t.getObject(i).equals(targetTuple.getObject(i));
                }
                
                if (matches) {
                    targetIter.remove();
                    foundValue = true;
                }
            }
            
            if (!foundValue) {
                fail("Result set had unexpected value: " + t);
            }
        }
        
        if (!targets.isEmpty()) {
            fail("Result set missing one or more expected values: " + targets.get(0));
        }
    }

}
