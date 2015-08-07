package com.scaleunlimited.snippets;

import java.util.List;

import cascading.tuple.Hasher;
import cascading.tuple.Tuple;
import cascading.tuple.util.TupleHasher;

public class BetterTupleHasher extends TupleHasher {

    private Hasher[] hashers;

    public int betterHashCode(Tuple tuple) {
        int hash = 0;

        List<Object> elements = Tuple.elements(tuple);

        for (int i = 0; i < elements.size(); i++) {
            Object element = elements.get(i);
            int elementHash = element != null ? hashers[i % hashers.length].hashCode(element) : 0;

            hash += elementHash;
            hash += (hash << 10);
            hash ^= (hash >> 6);
        }

        hash += (hash << 3);
        hash ^= (hash >> 11);
        hash += (hash << 15);
        
        return hash;
    }
}
