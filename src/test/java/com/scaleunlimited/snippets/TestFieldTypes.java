package com.scaleunlimited.snippets;

import org.junit.Test;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class TestFieldTypes {

    @Test
    public void testCoercion() {
        Fields f = new Fields("integer", int.class);
        
        TupleEntry te = new TupleEntry(f, Tuple.size(1));
        te.setInteger("integer", 1);
        te.setString("integer", "2");
        te.setLong("integer", Long.MAX_VALUE);
        System.out.println(te.getLong("integer"));
        
        te.setDouble("integer",  12.7);
        System.out.println(te.getLong("integer"));
        System.out.println(te.getObject("integer").getClass());
    }
}
