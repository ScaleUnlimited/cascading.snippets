package com.scaleunlimited.snippets;

import java.util.Iterator;

import com.scaleunlimited.cascading.NullContext;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.aggregator.First;
import cascading.operation.state.Counter;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class NoFieldGrouping extends SubAssembly {

    private static class CountGroups extends BaseOperation<NullContext> implements Buffer<NullContext> {
        
        private boolean _useFiledsNone;
        
        public CountGroups(boolean useFieldsNone) {
            super(useFieldsNone ? new Fields("count") : Fields.GROUP);
            _useFiledsNone = useFieldsNone;
        }
        
        @Override
        public void operate(FlowProcess flowProcess, BufferCall<NullContext> bufferCall) {
            if (_useFiledsNone) {
                int count = 0;
                Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
                while (iter.hasNext()) {
                    iter.next();
                    count += 1;
                }
                
                bufferCall.getOutputCollector().add(new Tuple(count));
            } else {
                bufferCall.getOutputCollector().add(bufferCall.getGroup());
            }
        }
    }
    
    public NoFieldGrouping(Pipe p, boolean useFieldsNone) {
        if (useFieldsNone) {
            p = new GroupBy(p, Fields.NONE);
        } else {
            p = new GroupBy(p);
        }
    
        p = new Every(p, new CountGroups(useFieldsNone), Fields.RESULTS);
        
        setTails(p);
    }
}
