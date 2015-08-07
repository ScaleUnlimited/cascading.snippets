package com.scaleunlimited.snippets;

import java.util.Iterator;

import com.scaleunlimited.cascading.NullContext;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class CountAndUniqueCountBuffer extends BaseOperation<NullContext> implements Buffer<NullContext> {
    
    public CountAndUniqueCountBuffer(String countFieldname, String uniqueCountFieldname) {
        // We accept one value, and output two fields.
        super(1, new Fields(countFieldname, uniqueCountFieldname));
    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall<NullContext> bufferCall) {
        Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
        
        int count = 0;
        int uniqueCount = 0;
        Object curValue = null;
        while (iter.hasNext()) {
            count +=1;
            
            Object newValue = iter.next().getObject(0);
            if ((curValue == null) || !curValue.equals(newValue)) {
                uniqueCount += 1;
                curValue = newValue;
            }
        }
        
        bufferCall.getOutputCollector().add(new Tuple(count, uniqueCount));
    }
    
    
}

