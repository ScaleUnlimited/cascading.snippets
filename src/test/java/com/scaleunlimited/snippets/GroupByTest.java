package com.scaleunlimited.snippets;

import org.junit.Test;

import cascading.flow.Flow;
import cascading.operation.Debug;
import cascading.operation.aggregator.Sum;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.hadoop.HadoopPlatform;

public class GroupByTest {

    @Test
    public void testFieldsAllWithHadoop() throws Exception {
        BasePlatform platform = new HadoopPlatform(GroupByTest.class);

        Tap sourceTap = new Lfs(new TextDelimited(), "build/test/GroupByTest/testFieldsAllWithHadoop/in");
        TupleEntryCollector write = sourceTap.openForWrite(platform.makeFlowProcess());
        write.add(new Tuple("bob", 1, 10));
        write.add(new Tuple("bob", 1, 50));
        write.add(new Tuple("john", 2, 20));
        write.add(new Tuple("bob", 6, 60));
        write.close();
        
        Pipe p = new Pipe("pipe");
        p = new Each(p, new Debug("pre-group", true));
        p = new GroupBy(p, new Fields(0, 1));
        p = new Every(p, new Fields(2), new Sum());
        p = new Each(p, new Debug("post-group", true));
        
        Tap sinkTap = new com.scaleunlimited.cascading.NullSinkTap();
        Flow flow = platform.makeFlowConnector().connect(sourceTap, sinkTap, p);
        flow.complete();
    }

}
