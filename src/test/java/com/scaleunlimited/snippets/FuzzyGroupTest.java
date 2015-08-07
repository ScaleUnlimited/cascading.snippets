package com.scaleunlimited.snippets;

import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Rename;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.InMemoryTap;

public class FuzzyGroupTest {

    private static final Fields LHS_FIELDS = new Fields("A1", "A2", "A3", "A4");
    private static final Fields RHS_FIELDS = new Fields("B1", "B2", "B3", "B4");
    
    private static class MakeGroupingKey extends BaseOperation<Void> implements Function<Void> {
        
        private Fields _groupFields;
        
        private transient Tuple _result;
        
        public MakeGroupingKey(Fields groupFields, Fields output) {
            super(output.append(new Fields("groupNum")));
            
            assert(output.size() == 1);
            
            _groupFields = groupFields;
        }
        
        @Override
        public void prepare(FlowProcess flowProcess, OperationCall<Void> operationCall) {
            super.prepare(flowProcess, operationCall);
            
            _result = Tuple.size(2);
        }
        
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<Void> functionCall) {
            // For each field in _groupFields, we want to emit a new grouping key
            
            int index = 0;
            for (Comparable fieldname : _groupFields) {
                _result.set(0, functionCall.getArguments().getObject(fieldname));
                _result.setInteger(1, index++);
                functionCall.getOutputCollector().add(_result);
            }
            
        }
    }
    
    @Test
    public void testFuzzyFieldJoin() throws Exception {
        InMemoryTap lhsTap = new InMemoryTap(LHS_FIELDS);
        TupleEntryCollector writer = lhsTap.openForWrite(new LocalFlowProcess());
        
        writer.add(new Tuple("1", "2", "3", "4"));
        writer.add(new Tuple("5", "6", "7", "8"));
        writer.close();

        InMemoryTap rhsTap = new InMemoryTap(RHS_FIELDS);
        writer = rhsTap.openForWrite(new LocalFlowProcess());
        
        writer.add(new Tuple("1", "12", "13", "14"));
        writer.add(new Tuple("15", "2", "17", "18"));
        writer.close();

        Pipe lhsPipe = new Pipe("lhs");
        lhsPipe = new Each(lhsPipe, new MakeGroupingKey(LHS_FIELDS, new Fields("lhsGroup")), Fields.ALL);
        lhsPipe = new Each(lhsPipe, DebugLevel.VERBOSE, new Debug("lhs with group", true));
        
        Pipe rhsPipe = new Pipe("rhs");
        rhsPipe = new Each(rhsPipe, new MakeGroupingKey(RHS_FIELDS, new Fields("rhsGroup")), Fields.ALL);
        rhsPipe = new Rename(rhsPipe, new Fields("groupNum"), new Fields("rhsGroupNum"));
        
        Pipe results = new CoGroup(lhsPipe, new Fields("lhsGroup", "groupNum"), rhsPipe, new Fields("rhsGroup", "rhsGroupNum"));
        
        // Now we might have duplicate results, so do a unique after retaining the fields we care about
        results = new Discard(results, new Fields("lhsGroup", "rhsGroup", "groupNum", "rhsGroupNum"));
        
        // How to do the unique is a bit of a problem, unless each tuple has a field or fields that can
        // be used to do the deduplication.
        
        results = new Each(results, DebugLevel.VERBOSE, new Debug("grouped", true));
        
        FlowDef flowDef = new FlowDef()
            .addSource(lhsPipe, lhsTap)
            .addSource(rhsPipe, rhsTap)
            .addTailSink(results, new NullSinkTap())
            .setDebugLevel(DebugLevel.VERBOSE);
        
        new LocalFlowConnector().connect(flowDef).complete();
    }

}
