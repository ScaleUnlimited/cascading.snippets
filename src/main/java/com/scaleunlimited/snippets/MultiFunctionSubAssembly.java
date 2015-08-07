package com.scaleunlimited.snippets;

import cascading.operation.Function;
import cascading.pipe.Each;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

@SuppressWarnings("serial")
public class MultiFunctionSubAssembly extends SubAssembly {

    @SuppressWarnings("rawtypes")
    public MultiFunctionSubAssembly(Pipe head, Function...functions) {
        Pipe[] pipes = new Pipe[functions.length];
        for (int i = 0; i < functions.length; i++) {
            pipes[i] = new Each(head, functions[i], Fields.REPLACE);
        }
        
        Pipe tail = new Merge(pipes);
        setTails(tail);
    }
}
