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
import cascading.tuple.TupleEntryCollector;

@SuppressWarnings("serial")
public class AddRank extends SubAssembly {

    @SuppressWarnings("rawtypes")
    private static class InsertRank extends BaseOperation implements Buffer {
        
        public InsertRank(Fields rankField) {
            super(rankField);
            
            if ((rankField.size() != 1) || !rankField.isDeclarator()) {
                throw new IllegalArgumentException("rankField should declare only one field name, got " + rankField);
            }
        }
        
        @Override
        public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
            // We'll get called with one group, that has every value sorted as requested.
            // Keep track of the rank. If we have duplicate values, assign them the same
            // rank, and then skip forward as appropriate.
            
            Tuple result = Tuple.size(1);
            TupleEntryCollector collector = bufferCall.getOutputCollector();
            Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
            
            int curRank = 1;
            int nextRank = 2;
            Tuple curTuple = null;
            
            while (iter.hasNext()) {
                TupleEntry nextTuple = iter.next();
                if (curTuple == null) {
                    curTuple = nextTuple.getTupleCopy();
                } else if (curTuple.equals(nextTuple.getTuple())) {
                    // Keep the current rank the same, advance next rank
                    nextRank++;
                } else {
                    curTuple = nextTuple.getTupleCopy();
                    curRank = nextRank++;
                }
                
                result.setInteger(0, curRank);
                collector.add(result);
            }
        }
    }
    
    
    public AddRank(Pipe p, Fields sortingFields, boolean reverseSort, Fields rankField) {
        p = new GroupBy(p, Fields.NONE, sortingFields, reverseSort);
        p = new Every(p, sortingFields, new InsertRank(rankField), Fields.ALL);
        
        setTails(p);
    }
}
