package com.scaleunlimited.snippets;

import java.util.Locale;

import org.junit.Test;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.InMemoryTap;

import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

public class RegexSplitGeneratorTest {

    @Test
    public void testImpatientSnippet() throws Exception {
        
        Tap tap = new InMemoryTap(new Fields("doc_id", "text"));
        TupleEntryCollector writer = tap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("doc01", "Voß rain shadow; is an Über"));
        writer.close();

        Pipe docPipe = new Pipe( "token" );
        Fields token = new Fields( "token" );
        Fields text = new Fields( "text" );
        RegexSplitGenerator splitter = new RegexSplitGenerator( token, "[ \\[\\]\\(\\),.]" );
        Fields fieldSelector = new Fields( "doc_id", "token" );
        docPipe = new Each( docPipe, text, splitter, fieldSelector );

        // define "ScrubFunction" to clean up the token stream
        Fields scrubArguments = new Fields( "doc_id", "token" );
        docPipe = new Each( docPipe, scrubArguments, new ScrubFunction( scrubArguments ), Fields.RESULTS );
        
        docPipe = new Each( docPipe, DebugLevel.VERBOSE, new Debug(true));
        
        FlowDef flowDef = new FlowDef();
        flowDef.addSource(docPipe, tap);
        flowDef.addTailSink(docPipe, new NullSinkTap());
        flowDef.setDebugLevel(DebugLevel.VERBOSE);
        
        new LocalFlowConnector().connect(flowDef).complete();
    }
    
    private static class ScrubFunction extends BaseOperation<Void> implements Function<Void> {

        public ScrubFunction( Fields fieldDeclaration )
        {
            super( 2, fieldDeclaration );
        }

        public String scrubText( String text )
        {
            return text.trim().toLowerCase();
        }

        public void operate( FlowProcess flowProcess, FunctionCall<Void> functionCall )
        {
        TupleEntry argument = functionCall.getArguments();
        String doc_id = argument.getString( 0 );
        String token = scrubText( argument.getString( 1 ) );

        if( token.length() > 0 )
          {
          Tuple result = new Tuple();
          result.add( doc_id );
          result.add( token );
          functionCall.getOutputCollector().add( result );
          }
        }

    }
}
