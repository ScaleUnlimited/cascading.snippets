package com.scaleunlimited.snippets;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.Debug;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Each;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.LeftJoin;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.InMemoryTap;

public class DistributedCrossProductTest {

    @Test
    public void test() throws Exception {
        Fields lhsFields = new Fields("lhs-key", "lhs-value");
        Pipe lhs = new Pipe("lhs");
        lhs = new Each(lhs, new Debug("lhs", true));
        
        Fields rhsFields = new Fields("rhs-key", "rhs-value");
        Pipe rhs = new Pipe("rhs");
        rhs = new Each(rhs, new Debug("rhs", true));
        
        //Join the two pipes
        Pipe joinPipe = new DistributedCrossProduct(lhs, lhsFields, rhs, rhsFields, 2);
        joinPipe = new Each(joinPipe, new Debug("joined",  true));

        InMemoryTap lhsTap = new InMemoryTap(lhsFields);
        TupleEntryCollector writer = lhsTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("lhs-1", "lhs-1-1"));
        writer.add(new Tuple("lhs-1", "lhs-1-2"));
        writer.add(new Tuple("lhs-2", "lhs-2-1"));
        writer.close();
        
        InMemoryTap rhsTap = new InMemoryTap(rhsFields);
        writer = rhsTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("rhs-1", "rhs-1-1"));
        writer.add(new Tuple("rhs-2", "rhs-2-1"));
        writer.close();
        
        FlowDef fd = new FlowDef();
        fd.addSource(lhs, lhsTap);
        fd.addSource(rhs, rhsTap);
        fd.addTailSink(joinPipe, new NullSinkTap());
        
        new LocalFlowConnector().connect(fd).complete();
    }

    // TODO run tests on both local and Hadoop
    // TODO test using multiple reducers in pseudo-distributed mode.
    @Test
    public void testSinglePipeHadoop() throws Exception {
        Fields fields = new Fields("key", "value");
        Pipe p = new Pipe("pipe");
        
        final int numReducers = 2;
        
        // Do a self-join
        Pipe joinPipe = new DistributedCrossProduct(p, numReducers);
        joinPipe = new Each(joinPipe, new Debug("joined",  true));

        Tap tap = new Lfs(new SequenceFile(fields), "build/test/DistributedCrossProductTest/testSinglePipeHadoop/in", SinkMode.REPLACE);
        TupleEntryCollector writer = tap.openForWrite(new HadoopFlowProcess());
        writer.add(new Tuple("1", "1-1"));
        writer.add(new Tuple("1", "1-2"));
        writer.add(new Tuple("2", "2-1"));
        writer.close();
        
        FlowDef fd = new FlowDef();
        fd.addSource(p, tap);
        fd.addTailSink(joinPipe, new NullSinkTap());
        
        new HadoopFlowConnector().connect(fd).complete();
    }
    
    // TODO run tests on both local and Hadoop
    // TODO test using multiple reducers in pseudo-distributed mode.
    @Test
    public void testSinglePipeLocal() throws Exception {
        Fields fields = new Fields("key", "value");
        Pipe p = new Pipe("pipe");
        
        final int numReducers = 2;
        
        // Do a self-join
        Pipe joinPipe = new DistributedCrossProduct(p, numReducers);
        joinPipe = new Each(joinPipe, new Debug("joined",  true));

        InMemoryTap tap = new InMemoryTap(fields);
        TupleEntryCollector writer = tap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("1", "1-1"));
        writer.add(new Tuple("1", "1-2"));
        writer.add(new Tuple("2", "2-1"));
        writer.close();
        
        FlowDef fd = new FlowDef();
        fd.addSource(p, tap);
        fd.addTailSink(joinPipe, new NullSinkTap());
        
        new LocalFlowConnector().connect(fd).complete();
    }

}
