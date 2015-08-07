package com.scaleunlimited.snippets;

import java.util.Iterator;

import com.scaleunlimited.cascading.NullContext;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class AllButLastBuffer extends BaseOperation<NullContext> implements Buffer<NullContext> {
    
    public AllButLastBuffer() {
        super(Fields.ARGS);
    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall<NullContext> bufferCall) {
        Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
        
        TupleEntry te = null;
        while (iter.hasNext()) {
            if (te != null) {
                bufferCall.getOutputCollector().add(te);
            }
            
            te = iter.next();
        }
    }
    
}
