package com.scaleunlimited.snippets;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.Debug;
import cascading.operation.regex.RegexReplace;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.InMemoryTap;

public class RegexReplaceTest {

    @Test
    public void test() throws Exception {
        Fields fields = new Fields("value", "count");
        InMemoryTap inTap = new InMemoryTap(fields);
        TupleEntryCollector writer = inTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("one", 3));
        writer.add(new Tuple("two", null));
        writer.close();

        Pipe pipe = new Pipe("pipe");
        pipe = new Each(pipe, new Debug("before", true));
        pipe = new Each(pipe, new Fields("count"), new RegexReplace(new Fields("count"), "null", "0"), Fields.REPLACE);
        pipe = new Each(pipe, new Debug("after", true));

        // Start the Flow
        FlowDef flowDef = FlowDef.flowDef()
                .addSource(pipe, inTap)
                .addTailSink(pipe, new NullSinkTap());
        new LocalFlowConnector().connect( flowDef ).complete(); 
    }

    @Test
    public void testRegex() {
        Pattern p = Pattern.compile("(?<f8>[^\\|]{0,10}+)");
        
        String text = "abcdefghijklmnopqrstuv";
        Matcher m = p.matcher(text);
        if (m.find()) {
            System.out.println(m.group(1));
        }
    }
}
