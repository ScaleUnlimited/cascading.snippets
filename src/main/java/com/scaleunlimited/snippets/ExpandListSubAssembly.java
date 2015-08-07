package com.scaleunlimited.snippets;

import com.scaleunlimited.cascading.NullContext;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings("serial")
public class ExpandListSubAssembly extends SubAssembly {

    @SuppressWarnings("rawtypes")
    private static class SplitList extends BaseOperation<NullContext> implements Function<NullContext> {
        
        private transient Tuple _result;
        
        public SplitList() {
            super(Fields.ARGS);
        }

        @Override
        public void prepare(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
            super.prepare(flowProcess, operationCall);
            _result = Tuple.size(1);
        }
        
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
            Tuple list = (Tuple)functionCall.getArguments().getTuple().getObject(0);
            
            for (Object o : list) {
                _result.set(0, o);
                functionCall.getOutputCollector().add(_result);
            }
        }
    }
    
    public ExpandListSubAssembly(Pipe p, Fields listField) {
        p = new Each(p, listField, new SplitList(), Fields.REPLACE);
        
        setTails(p);
    }
}
