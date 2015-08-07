package com.scaleunlimited.snippets;

import static org.junit.Assert.*;

import org.junit.Test;

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

public class AddGroupCountTest {

    @Test
    public void testTraditional() throws Exception {
        final Fields groupingFields = new Fields("NO", "SUBSCRIPTION");
        final Fields retainedFields = new Fields("CITY");
        final Fields otherFields = new Fields("bogus");
        
        Fields sourceFields = groupingFields.append(retainedFields).append(otherFields);
        InMemoryTap inTap = new InMemoryTap(sourceFields);
        TupleEntryCollector writer = inTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("a", 1, "LA", "xxx"));
        writer.add(new Tuple("a", 1, "SF", "zzz"));
        
        writer.add(new Tuple("a", 2, "NYC", "xyz"));
        
        writer.add(new Tuple("b", 2, "NYC", "x"));
        writer.close();

        Pipe pipe = new Pipe("in");
        pipe = new Each(pipe, new Debug("before", true));
        
        
        pipe = new AddGroupCount(pipe, groupingFields, retainedFields, "count");
        pipe = new Each(pipe, new Debug("after", true));

        new LocalFlowConnector().connect(inTap, new NullSinkTap(), pipe).complete();
    }

    @Test
    public void testSmallGroupSize() throws Exception {
        final Fields groupingFields = new Fields("NO", "SUBSCRIPTION");
        final Fields retainedFields = new Fields("CITY");
        final Fields otherFields = new Fields("bogus");
        
        Fields sourceFields = groupingFields.append(retainedFields).append(otherFields);
        InMemoryTap inTap = new InMemoryTap(sourceFields);
        TupleEntryCollector writer = inTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("a", 1, "LA", "xxx"));
        writer.add(new Tuple("a", 1, "SF", "zzz"));
        
        writer.add(new Tuple("a", 2, "NYC", "xyz"));
        
        writer.add(new Tuple("b", 2, "NYC", "x"));
        writer.close();

        Pipe pipe = new Pipe("in");
        pipe = new Each(pipe, new Debug("before", true));
        
        
        pipe = new AddGroupCount(pipe, groupingFields, retainedFields, "count", 2);
        pipe = new Each(pipe, new Debug("after", true));

        new LocalFlowConnector().connect(inTap, new NullSinkTap(), pipe).complete();
    }

}
