package com.scaleunlimited.snippets;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.junit.Test;

import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullSinkTap;

public class TestLog4JSettings {
    
    private static class MyLoggingFunction extends BaseOperation implements Filter {
        private static final Logger LOGGER = Logger.getLogger(TestLog4JSettings.MyLoggingFunction.class);
        
        @Override
        public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
            LOGGER.debug("Some message about " + filterCall.getArguments());
            return false;
        }
        
    }
    
    @Test
    public void testSettingLog4J() throws Exception {
        
        boolean debug = true;
        
        Fields sourceFields = new Fields("a", "b", "c");
        Tap inTap = new Hfs(new SequenceFile(sourceFields), "build/test/TestLog4JSettings/testSettingLog4J/", SinkMode.REPLACE);
        TupleEntryCollector writer = inTap.openForWrite(new HadoopFlowProcess());
        writer.add(new Tuple(1, "a", "first"));
        writer.add(new Tuple(2, "b", "second"));
        writer.add(new Tuple(3, "c", "third"));
        writer.close();

        Pipe pipe = new Pipe("in");
        pipe = new Each(pipe, DebugLevel.DEFAULT, new Debug(true));
        pipe = new Each(pipe, new MyLoggingFunction());
        
        FlowDef flowDef = new FlowDef();
        flowDef.addSource(pipe, inTap);
        flowDef.addTailSink(pipe, new NullSinkTap());
        flowDef.setDebugLevel(debug ? DebugLevel.VERBOSE : DebugLevel.NONE);
        
        Properties props = new Properties();
        props.put("log4j.logger", "cascading=INFO,com.scaleunlimited=INFO");
        FlowConnector connector = new HadoopFlowConnector(props);
        connector.connect(flowDef).complete();

    }

    @Test
    public void testSettingLog4JinLocalMode() throws Exception {
        
        boolean debug = true;
        
        Fields sourceFields = new Fields("a", "b", "c");
        Tap inTap = new FileTap(new TextLine(), "build/test/TestLog4JSettings/testSettingLog4JinLocalMode/", SinkMode.REPLACE);
        TupleEntryCollector writer = inTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple(1, "a", "first"));
        writer.add(new Tuple(2, "b", "second"));
        writer.add(new Tuple(3, "c", "third"));
        writer.close();

        Pipe pipe = new Pipe("in");
        pipe = new Each(pipe, DebugLevel.DEFAULT, new Debug(true));
        pipe = new Each(pipe, new MyLoggingFunction());
        
        FlowDef flowDef = new FlowDef();
        flowDef.addSource(pipe, inTap);
        flowDef.addTailSink(pipe, new NullSinkTap());
        flowDef.setDebugLevel(debug ? DebugLevel.VERBOSE : DebugLevel.NONE);
        
        Properties props = new Properties();
        props.put("log4j.logger", "cascading=INFO,com.scaleunlimited=INFO");
        FlowConnector connector = new LocalFlowConnector(props);
        connector.connect(flowDef).complete();

    }

}
