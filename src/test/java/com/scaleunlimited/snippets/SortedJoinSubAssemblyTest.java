package com.scaleunlimited.snippets;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.Debug;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.InMemoryTap;

public class SortedJoinSubAssemblyTest {

    @Test
    public void test() throws Exception {
        Pipe lhs = new Pipe("lhs");
        Fields lhsFields = new Fields("key", "value");
        lhs = new Each(lhs, new Debug("lhs", true));
        
        Pipe rhs = new Pipe("rhs");
        Fields rhsFields = new Fields("ID", "Name");
        rhs = new Each(rhs, new Debug("rhs", true));
        
        // Join the two pipes
        // TODO define a custom buffer to use here in the test
        Pipe joinPipe = new SortedJoinSubAssembly(lhs, lhsFields, rhs, rhsFields, new Fields("ID"), null);
        joinPipe = new Each(joinPipe, new Debug("joined",  true));

        /*
         * TODO match fields above
        InMemoryTap lhsTap = new InMemoryTap(new Fields("lhsKey", "lhsValue"));
        TupleEntryCollector writer = lhsTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("key1", "value1-1"));
        writer.add(new Tuple("key1", "value1-2"));
        writer.add(new Tuple("key2", "value2-1"));
        writer.close();
        
        InMemoryTap rhsTap = new InMemoryTap(new Fields("rhsKey", "rhsValue"));
        writer = rhsTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("keyA", "valueA-1"));
        writer.add(new Tuple("keyB", "valueB-1"));
        writer.add(new Tuple("keyB", "valueB-2"));
        writer.close();
        
        Map<String, Tap> sources = new HashMap<String, Tap>();
        sources.put(lhs.getName(), lhsTap);
        sources.put(rhs.getName(), rhsTap);
        
        new LocalFlowConnector().connect(sources, new NullSinkTap(), joinPipe).complete();
        */
    }

}
