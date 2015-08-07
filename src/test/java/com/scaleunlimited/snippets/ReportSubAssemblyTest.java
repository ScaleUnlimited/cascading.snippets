package com.scaleunlimited.snippets;

import static org.junit.Assert.*;

import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.Identity;
import cascading.operation.aggregator.First;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tap.local.PartitionTap;
import cascading.tap.local.TemplateTap;
import cascading.tap.partition.DelimitedPartition;
import cascading.tap.partition.Partition;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.InMemoryTap;

public class ReportSubAssemblyTest {

    @Test
    public void test() throws Exception {
        Fields groupingField = new Fields("contributor");
        Fields rawDetailFields = new Fields("value1", "value2", "value3");
        
        // This is the fields that we actually have in the incoming data. It contains
        // the field being used for grouping, plus the detail fields.
        Fields rawDataFields = groupingField.append(rawDetailFields);
        Tap tap = new InMemoryTap(rawDataFields);
        TupleEntryCollector writer = tap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("id1", 1, 10, "help"));
        writer.add(new Tuple("id1", 5, 50, "me"));
        writer.add(new Tuple("id2", 0, 17, "please"));
        writer.close();
        
        // Set up header for output - this is static.
        Fields headerFields = new Fields("record_type", "date", "report_name");
        TupleEntry headerRecord = new TupleEntry(headerFields, new Tuple("H", "20150108", "Test report"));
        
        // This needs to be the same as the fields in the records being processed, and adding in the record_type field.
        // The TupleEntry we pass in will have the leading "D" (record_type) field filled in, and placeholders
        // for the raw detail field values (filled in dynamically)
        Fields detailFields = new Fields("record_type").append(rawDetailFields);
        TupleEntry detailRecord = new TupleEntry(detailFields, new Tuple("D", null, null, null));
        
        final String trailerDetailCountFieldname = "detail_count";
        Fields trailerFields = new Fields("record_type", "report_name", trailerDetailCountFieldname);
        TupleEntry trailerRecord = new TupleEntry(trailerFields, new Tuple("T", "Test report", null));
        
        Pipe p = new Pipe("pipe");
        p = new ReportSubAssembly(p, groupingField, headerRecord, detailRecord, rawDetailFields, trailerRecord, trailerDetailCountFieldname);
        p = new Each(p, DebugLevel.VERBOSE, new Debug(true));
        
        // Now we need to partition by the grouping field(s). We've prefixed each record we emit with
        // a grouping key, so that's why we use Fields.FIRST, since we no longer have field names that
        // we can use (because the record, details, and trailer records can all have different # of fields)
        FileTap parentTap = new FileTap(new TextLine(), "build/test/ReportSubAssemblyTest/test/", SinkMode.REPLACE);
        Partition partition = new DelimitedPartition(Fields.FIRST);
        Tap reportTap = new TemplateTap(parentTap, "%s/", Fields.FIRST);

        FlowDef flowDef = new FlowDef()
            .addSource(p, tap)
            .addTailSink(p, reportTap)
            .setDebugLevel(DebugLevel.VERBOSE);
        
        new LocalFlowConnector().connect(flowDef).complete();
    }

}
