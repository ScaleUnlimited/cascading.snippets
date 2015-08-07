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
import cascading.pipe.joiner.OuterJoin;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class NoFieldHashJoin extends SubAssembly {

    public NoFieldHashJoin(Pipe lhs, Pipe rhs) {
        Pipe result = new HashJoin(lhs, Fields.NONE, rhs, Fields.NONE, new OuterJoin());
        
        setTails(result);
    }
}
