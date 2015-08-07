package com.scaleunlimited.snippets;

import java.util.regex.Pattern;

import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.Debug;
import cascading.operation.Function;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.InMemoryTap;

public class ExpressionFunctionTest {

    @Test
    public void test() throws Exception {
        Fields rankingsFields = new Fields("ratio");
        InMemoryTap inTap = new InMemoryTap(rankingsFields);
        TupleEntryCollector writer = inTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple(0.10));
        writer.close();

        Pipe p = new Pipe("percent");
        p = new Each(p, new Debug("before", true));
        Function fraudRate = new ExpressionFunction(new Fields("ratio"), "String.format(\"%.2f%%\", new Object[]{$0})", Double.class);
        p = new Each(p, new Fields("ratio"), fraudRate, Fields.REPLACE);
        p = new Each(p, new Debug("after", true));

        // Start the Flow
        FlowDef flowDef = FlowDef.flowDef()
                .addSource(p, inTap)
                .addTailSink(p, new NullSinkTap());
        new LocalFlowConnector().connect(flowDef).complete(); 
    }
    
    @Test
    public void testInsertOfDerivedField() throws Exception {
        Fields sourceFields = new Fields("task", "action");
        InMemoryTap inTap = new InMemoryTap(sourceFields);
        TupleEntryCollector writer = inTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("Feb_task1", "Insertion"));
        writer.add(new Tuple("Mar_task2", "Deletion"));
        writer.add(new Tuple("Nov_task3", "Post"));
        writer.close();

        Pipe p = new Pipe("percent");
        p = new Each(p, new Debug("before", true));
        
        String s = "Feb_tasks1";
        // Format a string with the first three character of the task field, plus the action field
        Function deriveMonth = new ExpressionFunction(new Fields("month"), "String.format(\"%s%s\", new Object[]{$0.substring(0,3), $1})", String.class);
        p = new Each(p, new Fields("task", "action"), deriveMonth, Fields.ALL);
        p = new Each(p, new Debug("after", true));

        // Start the Flow
        FlowDef flowDef = FlowDef.flowDef()
                .addSource(p, inTap)
                .addTailSink(p, new NullSinkTap());
        new LocalFlowConnector().connect(flowDef).complete(); 
    }
    

}
