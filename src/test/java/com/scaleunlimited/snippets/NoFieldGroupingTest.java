package com.scaleunlimited.snippets;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
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
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.DirectoryTap;
import com.scaleunlimited.cascading.local.InMemoryTap;
import com.scaleunlimited.cascading.local.KryoScheme;

public class NoFieldGroupingTest {

    @Test
    public void test() throws Exception {
        Tap tap = new InMemoryTap(new Fields("id", "value"));
        TupleEntryCollector writer = tap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple(1, "1-value1"));
        writer.add(new Tuple(1, "1-value1"));
        writer.add(new Tuple(1, "1-value2"));
        writer.add(new Tuple(2, "2-value1"));
        writer.close();
        
        Pipe p = new Pipe("pipe");
        Pipe results = new NoFieldGrouping(p, true);
        results = new Each(results, new Debug(true));
        
        FlowDef flowDef = new FlowDef();
        flowDef.addSource(p, tap);
        flowDef.addTailSink(results, new NullSinkTap());
        
        new LocalFlowConnector().connect(flowDef).complete();
    }

    @Test
    public void testInnerJoinWithNoFields() throws IOException {
        File tmpDirLHS = new File("build/test/NoFieldGroupingTest/testInnerJoinWithNoFields/lhs");
        tmpDirLHS.deleteOnExit();

        Fields lhsFields = new Fields("lhsCol1", "lhsCol2", "lhsCol3");
        String filenameLHS = tmpDirLHS.getAbsolutePath();
        DirectoryTap inLHS = new DirectoryTap(new KryoScheme(lhsFields), filenameLHS, SinkMode.REPLACE);
        TupleEntryCollector writer = inLHS.openForWrite(new LocalFlowProcess());

        writer.add(new Tuple(1, "abc", 100));
        writer.add(new Tuple(2, "def", 101));
        writer.add(new Tuple(3, "efg", 102));

        writer.close();

        File tempDirRHS = new File("build/test/NoFieldGroupingTest/testInnerJoinWithNoFields/rhs");
        tempDirRHS.deleteOnExit();

        Fields rhsFields = new Fields("rhsCol2", "rhsCol4");
        String filenameRHS = tempDirRHS.getAbsolutePath();
        DirectoryTap inRHS = new DirectoryTap(new KryoScheme(rhsFields), filenameRHS, SinkMode.REPLACE);
        writer = inRHS.openForWrite(new LocalFlowProcess());

        writer.add(new Tuple("abc", "str1"));
        writer.add(new Tuple("efg", "str2"));
        writer.close();

        // Now create a flow that does a CoGroup/InnerJoin on LHS/RHS and see what we get.

        Pipe lhs = new Pipe("lhs");
        lhs = new Each(lhs, new Debug("lhs", true));

        Pipe rhs = new Pipe("rhs");
        rhs = new Each(rhs, new Debug("rhs", true));

        Pipe joined = new CoGroup(lhs, new Fields("lhsCol2"), rhs, new Fields("rhsCol2"));
        joined = new Each(joined, new Debug("joined", true));

        joined = new Each(joined, lhsFields.append(new Fields("rhsCol4")), new Identity());
        joined = new Each(joined, new Debug("trimmed", true));
        
        Map<String, Tap> sourceTaps = new HashMap<String, Tap>();
        sourceTaps.put(lhs.getName(), inLHS);
        sourceTaps.put(rhs.getName(), inRHS);

        Tap sinkTap = new NullSinkTap(Fields.ALL);

        FlowConnector flowConnector = new LocalFlowConnector();
        Flow flow = flowConnector.connect(sourceTaps, sinkTap, joined);
        flow.complete();
    }


}
