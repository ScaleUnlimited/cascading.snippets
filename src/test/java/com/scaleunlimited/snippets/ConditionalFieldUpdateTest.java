package com.scaleunlimited.snippets;

import org.junit.Test;

import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Debug;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.InMemoryTap;

public class ConditionalFieldUpdateTest {

    private static final String ERROR_FLAG_FN = "has_error";
    private static final Fields ERROR_FLAG_FIELD = new Fields(ERROR_FLAG_FN);
    
    private static class SetErrorFlag extends BaseOperation implements Function {
        
        private Fields _sourceFields;
        private int _minValidValue;
        
        public SetErrorFlag(Fields sourceFields, int minValidValue) {
            super(sourceFields.append(ERROR_FLAG_FIELD));
            
            _sourceFields = sourceFields;
            _minValidValue = minValidValue;
        }

        @Override
        public void operate(FlowProcess process, FunctionCall call) {
            TupleEntry te = call.getArguments();
            // First see if we already have an error flag that's set.
            if (te.getFields().contains(ERROR_FLAG_FIELD)) {
                if (te.getBoolean(ERROR_FLAG_FN)) {
                    // All set with what we've got.
                   call.getOutputCollector().add(te);
                } else {
                    int value = te.getInteger("a");
                    if (value < _minValidValue) {
                        // We have to output a tuple with the error flag turned on.
                        Tuple result = te.selectTupleCopy(_sourceFields);
                        result.add(true);
                        call.getOutputCollector().add(result);
                    } else {
                        // All set with what we've got.
                        call.getOutputCollector().add(te);
                    }
                }
            } else {
                int value = te.getInteger("a");
                Tuple result = te.selectTupleCopy(_sourceFields);
                result.add(value < _minValidValue);
                call.getOutputCollector().add(result);
            }
        }
    }
    

    @Test
    public void testSettingErrorFlag() throws Exception {
        Fields sourceFields = new Fields("a");
        InMemoryTap inTap = new InMemoryTap(sourceFields);
        TupleEntryCollector writer = inTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple(0));
        writer.add(new Tuple(1));
        writer.add(new Tuple(2));
        writer.close();

        Pipe pipe = new Pipe("in");
        pipe = new Each(pipe, new SetErrorFlag(sourceFields, 1));
        pipe = new Each(pipe, new Debug("first test", true));

        pipe = new Each(pipe, new SetErrorFlag(sourceFields, 2));
        pipe = new Each(pipe, new Debug("second test", true));

        new LocalFlowConnector().connect(inTap, new NullSinkTap(), pipe).complete();
    }

    private static class SetErrorFlag2 extends BaseOperation implements Function {
        
        private int _minValidValue;
        
        public SetErrorFlag2(int minValidValue) {
            super(Fields.ARGS);
            
            _minValidValue = minValidValue;
        }

        @Override
        public void operate(FlowProcess process, FunctionCall call) {
            TupleEntry te = call.getArguments();
            
            // First see if the error flag is already set.
            if (te.getBoolean(ERROR_FLAG_FN)) {
                // All set with what we've got.
                call.getOutputCollector().add(te);
            } else {
                int value = te.getInteger("a");
                if (value < _minValidValue) {
                    // We have to output a tuple with the error flag turned on.
                    Tuple result = te.getTupleCopy();
                    result.setBoolean(te.getFields().getPos(ERROR_FLAG_FN), true);
                    call.getOutputCollector().add(result);
                } else {
                    // All set with what we've got.
                    call.getOutputCollector().add(te);
                }
            }
        }
    }
    
    @Test
    public void testSettingErrorFlag2() throws Exception {
        Fields sourceFields = new Fields("a", ERROR_FLAG_FN);
        InMemoryTap inTap = new InMemoryTap(sourceFields);
        TupleEntryCollector writer = inTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple(0, false));
        writer.add(new Tuple(1, false));
        writer.add(new Tuple(2, false));
        writer.close();

        Pipe pipe = new Pipe("in");
        pipe = new Each(pipe, sourceFields, new SetErrorFlag2(1));
        pipe = new Each(pipe, new Debug("first test", true));

        pipe = new Each(pipe, sourceFields, new SetErrorFlag2(2));
        pipe = new Each(pipe, new Debug("second test", true));

        new LocalFlowConnector().connect(inTap, new NullSinkTap(), pipe).complete();
    }



}
