package com.scaleunlimited.snippets;

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class SameBy extends SubAssembly {

    public SameBy(Pipe[] pipes, Fields groupingFields, Fields sameFields, boolean reverseOrder) {
        Pipe p = new GroupBy(pipes, groupingFields, sameFields, reverseOrder);
        p = new Every(p, new SameValues(sameFields), Fields.RESULTS);
        setTails(p);
    }
    
    @SuppressWarnings("serial")
    private static class SameValues extends BaseOperation<Void> implements Buffer<Void> {
        
        private Fields _sameFields;
        
        public SameValues(Fields sameFields) {
            super(Fields.ARGS);
            
            _sameFields = sameFields;
        }

        @Override
        public void operate(FlowProcess flowProcess, BufferCall<Void> bufferCall) {

            Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
            TupleEntry te = iter.next();
            bufferCall.getOutputCollector().add(te);

            Tuple sameValue = te.selectTupleCopy(_sameFields);
            while (iter.hasNext()) {
                te = iter.next();
                if (sameValue.equals(te.selectTuple(_sameFields))) {
                    bufferCall.getOutputCollector().add(te);
                } else {
                    break;
                }
            }
        }
        
        
    }
}
