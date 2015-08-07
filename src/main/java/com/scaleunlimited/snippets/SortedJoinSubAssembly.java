package com.scaleunlimited.snippets;

import cascading.operation.Buffer;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

public class SortedJoinSubAssembly extends SubAssembly {

    public SortedJoinSubAssembly(Pipe lhs, Fields lhsFields, Pipe rhs, Fields rhsFields, Fields rhsSortField, Buffer joinOp) {
        // Add all of the rhs fields as nulls to the lhs pipe
        lhs = new Each(lhs, new Insert(rhsFields, new Object[rhsFields.size()]), Fields.ALL);
        
        // Add all of the lhs fields as nulls to the rhs pipe
        rhs = new Each(rhs, new Insert(lhsFields, new Object[lhsFields.size()]), Fields.ALL);
        
        // CoGroup on no fields, sorting by rhsSortField
        Pipe joined = new GroupBy(Pipe.pipes(lhs, rhs), Fields.NONE, rhsSortField);
        joined = new Every(joined, joinOp);
        
        setTails(joined);
    }
}
