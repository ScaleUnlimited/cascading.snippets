package com.scaleunlimited.snippets;

import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.scaleunlimited.cascading.NullSinkTap;

public class SequenceFileTest {

    @Test
    public void testReadingUnknownFields() throws Exception {
        Fields fields = new Fields("value", "count");
        String path = "build/test/SequenceFileTest/testReadingUnknownFields";
        Tap tap = new Hfs(new SequenceFile(fields), path, SinkMode.REPLACE);
        
        TupleEntryCollector writer = tap.openForWrite(new HadoopFlowProcess());
        writer.add(new Tuple("one", 3));
        writer.add(new Tuple("two", null));
        writer.close();

        Pipe pipe = new Pipe("pipe");
        pipe = new Each(pipe, DebugLevel.VERBOSE, new Debug(true));
        
        Tap sourceTap = new Hfs(new SequenceFile(Fields.UNKNOWN), path, SinkMode.KEEP);
        TupleEntryIterator iter = sourceTap.openForRead(new HadoopFlowProcess());
        while (iter.hasNext()) {
            System.out.println(iter.next().getTuple());
        }
        iter.close();
        
        // Start the Flow
        FlowDef flowDef = FlowDef.flowDef()
                .addSource(pipe, sourceTap)
                .addTailSink(pipe, new NullSinkTap())
                .setDebugLevel(DebugLevel.VERBOSE);
        
        new HadoopFlowConnector().connect(flowDef).complete();
        
        
    }

}
