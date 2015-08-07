package com.scaleunlimited.snippets;

import java.util.Iterator;

import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.InMemoryTap;

public class SplitUsingEntireGroup {

    @Test
    public void test() throws Exception {
        Fields fields = new Fields("identifier", "col1", "col2", "col3", "count");
        
        InMemoryTap inTap = new InMemoryTap(fields);
        TupleEntryCollector writer = inTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("identifier1","a", "a", "a", 12));
        writer.add(new Tuple("identifier1","a", "b", "a", 10));
        writer.add(new Tuple("identifier1","a", "b", "c", 8));
        
        writer.add(new Tuple("identifier2","a", "b", "a", 3));
        writer.add(new Tuple("identifier2","b", "b", "b", 8));
        writer.close();
        
        Pipe p = new Pipe("pipe");
        
        // Split off a pipe where we calculate the 'flag' value for each identifier
        Pipe flagged = new Pipe("flagged", p);
        flagged = new GroupBy(flagged, new Fields("identifier"));
        flagged = new Every(flagged, new FlagGroupEntries(), Fields.RESULTS);
        
        // Join the flag back in (we could use HashJoin here)
        p = new CoGroup(p, new Fields("identifier"),
                        flagged, new Fields("identifier_flagged"));
        p = new Discard(p, new Fields("identifier_flagged"));
        
        // Now split into two pipes and use the flag to filter.
        Pipe truePipe = new Pipe("true pipe", p);
        truePipe = new Each(truePipe, new Fields("flag"), new ExpressionFilter("!$0", Boolean.class));
        truePipe = new Discard(truePipe, new Fields("flag"));
        truePipe = new Each(truePipe, DebugLevel.VERBOSE, new Debug("true pipe", true));
        
        Pipe falsePipe = new Pipe("false pipe", p);
        falsePipe = new Each(falsePipe, new Fields("flag"), new ExpressionFilter("$0", Boolean.class));
        falsePipe = new Discard(falsePipe, new Fields("flag"));
        falsePipe = new Each(falsePipe, DebugLevel.VERBOSE, new Debug("false pipe", true));

        // Start the Flow
        FlowDef flowDef = FlowDef.flowDef()
                .addSource(p, inTap)
                .addTailSink(truePipe, new NullSinkTap())
                .addTailSink(falsePipe, new NullSinkTap())
                .setDebugLevel(DebugLevel.VERBOSE);
        new LocalFlowConnector().connect(flowDef).complete(); 
    }

    /**
     * Output identifier and flag, which is true if the sum of the count fields for the group
     * is >= 30.
     *
     */
    private static class FlagGroupEntries extends BaseOperation<Void> implements Buffer<Void> {
        public FlagGroupEntries() {
            super(new Fields("identifier_flagged", "flag"));
        }
        
        @Override
        public void operate(FlowProcess flowProcess, BufferCall<Void> bufferCall) {
            String identifier = bufferCall.getGroup().getString("identifier");
            Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
            
            int total = 0;
            while (iter.hasNext()) {
                TupleEntry te = iter.next();
                total += te.getInteger("count");
            }
            
            bufferCall.getOutputCollector().add(new Tuple(identifier, total >= 30));
        }
    }
}
