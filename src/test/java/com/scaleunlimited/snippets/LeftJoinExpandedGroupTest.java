package com.scaleunlimited.snippets;

import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.Identity;
import cascading.operation.filter.FilterNull;
import cascading.operation.filter.Not;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.LeftJoin;
import cascading.pipe.joiner.RightJoin;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.InMemoryTap;

public class LeftJoinExpandedGroupTest {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testLeftAndMatchingRight() throws Exception {
        
        Fields lhsFields = new Fields("lhs-id");
        Tap lhsSource = new InMemoryTap(lhsFields);
        TupleEntryCollector lhsWriter = lhsSource.openForWrite(new LocalFlowProcess());
        
        lhsWriter.add(new Tuple("a"));
        lhsWriter.add(new Tuple("b"));
        lhsWriter.add(new Tuple("c"));
        lhsWriter.add(new Tuple("i"));
        lhsWriter.close();
        
        Fields rhsFields = new Fields("rhs-id");
        Tap rhsSource = new InMemoryTap(rhsFields);
        TupleEntryCollector rhsWriter = rhsSource.openForWrite(new LocalFlowProcess());
        
        rhsWriter.add(new Tuple("a"));
        rhsWriter.add(new Tuple("h"));
        rhsWriter.add(new Tuple("i"));
        rhsWriter.add(new Tuple("k"));
        rhsWriter.add(new Tuple("d"));
        rhsWriter.add(new Tuple("Z"));
        rhsWriter.close();
        
        Pipe lhsPipe = new Pipe("lhs");
        lhsPipe = new Each(lhsPipe, DebugLevel.VERBOSE, new Debug("lhs", true));

        Pipe rhsPipe = new Pipe("rhs");
        rhsPipe = new Each(rhsPipe, DebugLevel.VERBOSE, new Debug("rhs", true));

        Pipe joinPipe = new CoGroup(lhsPipe, lhsFields, rhsPipe, rhsFields, new LeftJoin());
        joinPipe = new Each(joinPipe, DebugLevel.VERBOSE, new Debug("joined", true));
        
        Pipe lhsValid = new Pipe("lhs-valid", joinPipe);
        lhsValid = new Each(lhsValid, lhsFields, new Identity(new Fields("id")));
        
        Pipe rhsValid = new Pipe("rhs-valid", joinPipe);
        rhsValid = new Each(rhsValid, rhsFields, new Identity(new Fields("id")));
        rhsValid = new Each(rhsValid, new FilterNull());
        
        Pipe result = new Merge(lhsValid, rhsValid);
        result = new Each(result, new Debug("result"));

        FlowDef fd = new FlowDef();
        fd.addSource(lhsPipe, lhsSource);
        fd.addSource(rhsPipe, rhsSource);
        fd.addTailSink(result, new NullSinkTap());
        
        new LocalFlowConnector().connect(fd).complete();
        
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testRightOnly() throws Exception {
        
        Fields lhsFields = new Fields("lhs-id");
        Tap lhsSource = new InMemoryTap(lhsFields);
        TupleEntryCollector lhsWriter = lhsSource.openForWrite(new LocalFlowProcess());
        
        lhsWriter.add(new Tuple("a"));
        lhsWriter.add(new Tuple("b"));
        lhsWriter.add(new Tuple("c"));
        lhsWriter.add(new Tuple("i"));
        lhsWriter.close();
        
        Fields rhsFields = new Fields("rhs-id");
        Tap rhsSource = new InMemoryTap(rhsFields);
        TupleEntryCollector rhsWriter = rhsSource.openForWrite(new LocalFlowProcess());
        
        rhsWriter.add(new Tuple("a"));
        rhsWriter.add(new Tuple("h"));
        rhsWriter.add(new Tuple("i"));
        rhsWriter.add(new Tuple("k"));
        rhsWriter.add(new Tuple("d"));
        rhsWriter.add(new Tuple("Z"));
        rhsWriter.close();
        
        Pipe lhsPipe = new Pipe("lhs");
        lhsPipe = new Each(lhsPipe, DebugLevel.VERBOSE, new Debug("lhs", true));

        Pipe rhsPipe = new Pipe("rhs");
        rhsPipe = new Each(rhsPipe, DebugLevel.VERBOSE, new Debug("rhs", true));

        Pipe joinPipe = new CoGroup(lhsPipe, lhsFields, rhsPipe, rhsFields, new RightJoin());
        joinPipe = new Each(joinPipe, DebugLevel.VERBOSE, new Debug("joined", true));
        
        joinPipe = new Each(joinPipe, new Fields("lhs-id"), new Not(new FilterNull()));
        joinPipe = new Retain(joinPipe, new Fields("rhs-id"));
        joinPipe = new Each(joinPipe, new Debug("result"));

        FlowDef fd = new FlowDef();
        fd.addSource(lhsPipe, lhsSource);
        fd.addSource(rhsPipe, rhsSource);
        fd.addTailSink(joinPipe, new NullSinkTap());
        
        new LocalFlowConnector().connect(fd).complete();
        
    }

}
