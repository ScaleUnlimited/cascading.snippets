package com.scaleunlimited.snippets;

import cascading.flow.FlowDef;
import cascading.operation.Identity;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Rename;
import cascading.pipe.joiner.OuterJoin;
import cascading.tap.Tap;
import cascading.tuple.Fields;

public class TapJoiner {

    private static final String RIGHT_SIDE_JOIN_FIELDNAME = "TapJoiner_RHS_Fieldname";
    
    @SuppressWarnings("rawtypes")
    public static FlowDef createFlow(Tap<?,?,?> tap1, Tap<?,?,?> tap2, Tap<?,?,?>outTap) {
        Pipe pipe1 = new Pipe("pipe1");
        Pipe pipe2 = new Pipe("pipe2");
    
        // Avoid field name collision
        pipe2 = new Rename(pipe2, new Fields("id"), new Fields(RIGHT_SIDE_JOIN_FIELDNAME));
        
        Pipe finalPipe = new CoGroup(   pipe1, new Fields("id"),
                                        pipe2, new Fields(RIGHT_SIDE_JOIN_FIELDNAME),
                                        new OuterJoin());

        // Get rid of the RHS joiner field.
        Fields rhsFields = tap2.getSourceFields().subtract(new Fields("id"));
        Fields residualFields = tap1.getSourceFields().append(rhsFields);
        finalPipe = new Each(finalPipe, residualFields, new Identity());
        
        FlowDef flowDef = new FlowDef();
        flowDef.addSource(pipe1,tap1);
        flowDef.addSource(pipe2,tap2);
        flowDef.addTailSink(finalPipe, outTap);

        return flowDef;
    }
}
