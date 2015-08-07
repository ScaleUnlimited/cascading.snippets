package com.scaleunlimited.snippets;

import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.InMemoryTap;

public class SplitAndReplicateTest {

    @Test
    public void test() throws Exception {
        Fields sourceFields = new Fields("values", "keyword");
        InMemoryTap inTap = new InMemoryTap(sourceFields);
        TupleEntryCollector writer = inTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("1:2:3", "abc"));
        writer.add(new Tuple("12345:784512:7854123", "xyz"));
        writer.close();

        Pipe pipe = new Pipe("in");
        pipe = new Each(pipe, new Fields("values"), new RegexSplitGenerator(new Fields("value"), ":"), Fields.SWAP);
        pipe = new Each(pipe, DebugLevel.VERBOSE, new Debug("after", true));

        FlowDef flowDef = new FlowDef()
            .addSource(pipe, inTap)
            .addTailSink(pipe, new NullSinkTap())
            .setDebugLevel(DebugLevel.VERBOSE);
        new LocalFlowConnector().connect(flowDef).complete();
    }

}
