package com.scaleunlimited.snippets;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.*;

import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public class AllButLastBufferTest {

    @Test
    public void testBufferAfterBuffer() throws Exception {
        final Fields groupField = new Fields("user");
        final Fields testFields = new Fields("user", "value");
        
        String in = "build/test/BufferTest/testBufferAfterBuffer/in";
        String out = "build/test/BufferTest/testBufferAfterBuffer/out";

        Lfs sourceTap = new Lfs(new SequenceFile(testFields), in, SinkMode.REPLACE);
        TupleEntryCollector write = sourceTap.openForWrite(new HadoopFlowProcess());
        write.add(new Tuple("user1", 12));
        write.add(new Tuple("user1", 11));
        write.add(new Tuple("user2", 21));
        write.close();

        Pipe pipe = new Pipe("test");
        pipe = new GroupBy(pipe, groupField);
        pipe = new Every(pipe, new AllButLastBuffer(), Fields.RESULTS);
        pipe = new Every(pipe, new AllButLastBuffer(), Fields.RESULTS);
        
        Lfs sinkTap = new Lfs(new SequenceFile(testFields), out, SinkMode.REPLACE);
        
        Flow flow = new HadoopFlowConnector().connect(sourceTap, sinkTap, pipe);
        flow.complete();

        int count = 0;
        TupleEntryIterator iter = sinkTap.openForRead(new HadoopFlowProcess());
        assertTrue(iter.hasNext());
        iter.next();
        assertFalse(iter.hasNext());
    }
    

}
