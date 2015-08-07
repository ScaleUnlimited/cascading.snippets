package com.scaleunlimited.snippets;

import org.junit.Test;

import cascading.CascadingTestCase;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleListCollector;

public class CascadingTestCaseTest extends CascadingTestCase {

    static class CalcRisk extends BaseOperation implements Function {

        public CalcRisk() {
            super(1, new Fields("risk"));
        }
            
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            TupleEntry arguments = functionCall.getArguments();
            Tuple result = new Tuple(arguments.getInteger(0) < 50 ? "low" : "high");
            functionCall.getOutputCollector().add(result);
        }
    }

    @Test
    public void testCalcRiskFunction() {
        // We can pass either a Tuple (if the operation uses positions) or a TupleEntry to
        // the operation being tested.
        TupleListCollector result = invokeFunction(new CalcRisk(), new Tuple(37), new Fields("risk"));
        assertEquals("low", result.iterator().next().getString(0));
    }
    
}
