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

public class ExpandListSubAssemblyTest {

    @Test
    public void test() throws Exception {
        Fields sourceFields = new Fields("id", "name", "ListItem");
        InMemoryTap inTap = new InMemoryTap(sourceFields);
        TupleEntryCollector writer = inTap.openForWrite(new LocalFlowProcess());
        
        writer.add(new Tuple(1, "A1", new Tuple("B1", "C1", "D1", "E1")));
        writer.add(new Tuple(2, "A2", new Tuple("B2", "C2")));
        writer.add(new Tuple(3, "A3", new Tuple("B3", "C3", "D3")));
        writer.close();

        Pipe pipe = new Pipe("in");
        pipe = new ExpandListSubAssembly(pipe, new Fields("ListItem"));
        pipe = new Each(pipe, new Debug(true));

        new LocalFlowConnector().connect(inTap, new NullSinkTap(), pipe).complete();
    }

}
