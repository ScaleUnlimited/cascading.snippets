package com.scaleunlimited.snippets;

import junit.framework.Assert;

import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.flow.planner.PlannerException;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.InMemoryTap;

public class OperatorExpectedArgsTest {

    static class CalcRisk extends BaseOperation implements Function {

        public CalcRisk(int numExpectedArgs) {
            super(numExpectedArgs, new Fields("risk"));
        }
            
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            TupleEntry arguments = functionCall.getArguments();
            Tuple result = new Tuple(arguments.getInteger(0) < 50 ? "low" : "high");
            functionCall.getOutputCollector().add(result);
        }
    }

    @Test
    public void testExpectedArgs() throws Exception {
        Tap sourceTap = new InMemoryTap(new Fields("name", "age"));
        TupleEntryCollector writer = sourceTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("ken", 37));
        writer.close();
        
        Pipe p = new Pipe("test");
        p = new Each(p, new Fields("age"), new CalcRisk(3));
        
        FlowDef flowDef = new FlowDef()
            .setName("test expected args")
            .addSource(p, sourceTap)
            .addTailSink(p, new NullSinkTap());
        
        try {
            new LocalFlowConnector().connect(flowDef);
            Assert.fail("Should have thrown PlannerException");
        } catch (PlannerException e) {
            // Expected
        }
        
        
    }

}
