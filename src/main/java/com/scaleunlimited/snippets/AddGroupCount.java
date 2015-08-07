package com.scaleunlimited.snippets;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.pipe.CoGroup;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class AddGroupCount extends SubAssembly {

    public AddGroupCount(Pipe p, Fields groupingFields, Fields retainedFields, String countFieldName) {
        Fields mungedFields = new Fields();
        for (Comparable fieldname : groupingFields) {
            mungedFields = mungedFields.append(new Fields("AddGroupCount_" + fieldname));
        }

        Pipe combinerPipe = new Pipe("Combiner", p);
        combinerPipe = new Retain(combinerPipe, groupingFields.append(retainedFields));
        
        p = new CountBy(p, groupingFields, new Fields(countFieldName));
        p = new Rename(p, groupingFields, mungedFields);
        
        Pipe result = new CoGroup(combinerPipe, groupingFields, p, mungedFields);
        result = new Retain(result, groupingFields.append(retainedFields).append(new Fields(countFieldName)));
        
        setTails(result);
    }

    private static class InjectCount extends BaseOperation<Void> implements Buffer<Void> {
        
        private Fields _groupingFields;
        private Fields _retainedFields;
        
        public InjectCount(Fields groupingFields, Fields retainedFields, String countFieldname) {
            super(groupingFields.append(retainedFields.append(new Fields(countFieldname))));
            
            _groupingFields = groupingFields;
            _retainedFields = retainedFields;
        }
        
        @Override
        public void operate(FlowProcess flowProcess, BufferCall<Void> bufferCall) {
            List<Tuple> groupValues = new ArrayList<Tuple>();
            Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
            
            int groupSize = 0;
            while (iter.hasNext()) {
                groupSize += 1;
                TupleEntry te = iter.next();
                Tuple t = new Tuple();
                t.addAll(te.selectTuple(_retainedFields));
                groupValues.add(t);
            }
            
            for (Tuple t : groupValues) {
                t.add(groupSize);
                bufferCall.getOutputCollector().add(new Tuple(groupSize));
            }
        }
    }
    
    public AddGroupCount(Pipe p, Fields groupingFields, Fields retainedFields, String countFieldName, int maxGroupSize) {
        
        // TODO Make sure retainedFields and groupingFields are disjoint
        p = new GroupBy(p, groupingFields, retainedFields);
        p = new Every(p, new InjectCount(groupingFields, retainedFields, countFieldName), Fields.RESULTS);

        setTails(p);
    }

}
