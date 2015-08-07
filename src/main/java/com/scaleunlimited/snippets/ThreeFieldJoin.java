package com.scaleunlimited.snippets;

import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.operation.aggregator.First;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

@SuppressWarnings("serial")
public class ThreeFieldJoin extends SubAssembly {

    public static final Fields LHS_FIELDS = new Fields("Key", "Data");
    public static final Fields RHS_FIELDS = new Fields("key1", "key2", "key3");
    
    private static final Fields RANK_FIELD = new Fields("rank");
    
    public ThreeFieldJoin(Pipe lhsPipe, Pipe threeFieldsPipe) {
        
        Pipe key1Pipe = new Pipe("key1Pipe", threeFieldsPipe);
        key1Pipe = new Each(key1Pipe, new Insert(RANK_FIELD, 1), Fields.ALL);
        Pipe key1Join = new CoGroup(lhsPipe, new Fields("Key"),
                                    key1Pipe, new Fields("key1"));
        
        Pipe key2Pipe = new Pipe("key2Pipe", threeFieldsPipe);
        key2Pipe = new Each(key2Pipe, new Insert(RANK_FIELD, 2), Fields.ALL);
        Pipe key2Join = new CoGroup(lhsPipe, new Fields("Key"),
                                    key2Pipe, new Fields("key2"));
        
        Pipe key3Pipe = new Pipe("key3Pipe", threeFieldsPipe);
        key3Pipe = new Each(key3Pipe, new Insert(RANK_FIELD, 3), Fields.ALL);
        Pipe key3Join = new CoGroup(lhsPipe, new Fields("Key"),
                                    key3Pipe, new Fields("key3"));
        
        
        Pipe resultPipe = new GroupBy(Pipe.pipes(key1Join, key2Join, key3Join), new Fields("Key"), RANK_FIELD);
        resultPipe = new Every(resultPipe, new First(), Fields.RESULTS);
        resultPipe = new Each(resultPipe, LHS_FIELDS.append(RHS_FIELDS), new Identity());
        
        setTails(resultPipe);
    }
}
