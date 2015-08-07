package com.scaleunlimited.snippets;

import java.lang.reflect.Type;

import org.junit.Test;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class TupleEntryTest {

    @Test
    public void testSetWithoutFieldTypes() {
        final Fields fields1 = new Fields("key", "value");
        TupleEntry te1 = new TupleEntry(fields1, new Tuple("a", 1));
        
        final Fields fields2 = fields1.append(new Fields("extra"));
        TupleEntry te2 = new TupleEntry(fields2, Tuple.size(fields2.size()));
        
        // Try to set the two fields from te1 in te2.
        te2.set(te1);
    }

    @Test
    public void testSetWithFieldTypes() {
        final Fields fields1 = new Fields(new String[] {"key", "value"}, new Type[] {String.class, Integer.class});
        TupleEntry te1 = new TupleEntry(fields1, new Tuple("a", 1));
        
        final Fields fields2 = fields1.append(new Fields("extra", String.class));
        TupleEntry te2 = new TupleEntry(fields2, Tuple.size(fields2.size()));
        
        // Try to set the two fields from te1 in te2.
        te2.set(te1);
    }
}
