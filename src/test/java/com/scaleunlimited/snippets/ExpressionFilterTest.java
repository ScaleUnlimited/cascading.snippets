package com.scaleunlimited.snippets;

import java.net.URL;

import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.Debug;
import cascading.operation.Identity;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.InMemoryTap;

public class ExpressionFilterTest {

    @Test
    public void test() throws Exception {
        Fields rankingsFields = new Fields("pageURL", "pageRank", "avgDuration");
        InMemoryTap inTap = new InMemoryTap(rankingsFields);
        TupleEntryCollector writer = inTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("http://domain.com/page1", 4.5, 100));
        writer.add(new Tuple("http://domain.com/page2", 2, 50));
        writer.add(new Tuple("http://domain.com/page3", "8", 10));
        writer.close();

        Pipe rankingsPipe = new Pipe("rankings");
        rankingsPipe = new Each(rankingsPipe, new Debug("before", true));
        rankingsPipe = new Each(rankingsPipe, new Fields("pageRank"), new ExpressionFilter("$0 < 5", Integer.class));
        rankingsPipe = new Each(rankingsPipe, new Debug("after", true));

        // Start the Flow
        FlowDef flowDef = FlowDef.flowDef()
                .addSource(rankingsPipe, inTap)
                .addTailSink(rankingsPipe, new NullSinkTap());
        new LocalFlowConnector().connect( flowDef ).complete(); 
    }
    
}
