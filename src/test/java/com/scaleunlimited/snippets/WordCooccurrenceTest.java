package com.scaleunlimited.snippets;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.InMemoryTap;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.Debug;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

public class WordCooccurrenceTest {

    @Test
    public void test() throws Exception {
        Tap sourceTap = new InMemoryTap(new Fields("docid", "text"));
        TupleEntryCollector writer = sourceTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("D1", "The quick brown fox jumped over the lazy dog"));
        writer.add(new Tuple("D2", "The boy ran to the market"));
        writer.add(new Tuple("D3", "The girl walked over to the cat"));
        writer.close();
        
        Set<String> listA = new HashSet<String>();
        Collections.addAll(listA, "fox chicken hen dog cat".split(" "));
        
        Set<String> listB = new HashSet<String>();
        Collections.addAll(listB, "ran jumped walked crawled".split(" "));
        
        Pipe p = new Pipe("cooccurrence test");
        p = new WordCooccurrence(p, "docid", "text", listA, listB);
        p = new Each(p, new Debug(true));
        
        FlowDef fd = new FlowDef();
        fd.addSource(p, sourceTap);
        
        fd.addTailSink(p, new NullSinkTap());
        new LocalFlowConnector().connect(fd).complete();
    }

}
