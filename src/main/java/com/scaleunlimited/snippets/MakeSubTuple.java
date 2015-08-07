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

public class MakeSubTuple extends SubAssembly {

    private static class InjectTuple extends BaseOperation<NullContext> implements Function<NullContext> {
        
        private transient Tuple _result;
        
        public InjectTuple(Fields target) {
            super(target);
        }
        
        @Override
        public void prepare(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
            super.prepare(flowProcess, operationCall);
            
            _result = Tuple.size(1);
        }
        
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
            // Make a clone, as there's no way currently for us to know if downstream we've got
            // a map-size aggregator that does a shallow copy of the Tuple we're emitting.
            _result.set(0, functionCall.getArguments().getTupleCopy());
            functionCall.getOutputCollector().add(_result);
        }
    }
    
    public MakeSubTuple(Pipe p, Fields sourceFields, Fields targetField) {
        p = new Each(p, sourceFields, new InjectTuple(targetField), Fields.SWAP);

        setTails(p);
    }
}
