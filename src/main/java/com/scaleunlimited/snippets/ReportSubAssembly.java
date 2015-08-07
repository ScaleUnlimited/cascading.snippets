package com.scaleunlimited.snippets;

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Generate output suitable for a report with a header, N detail records,
 * and a trailer.
 * 
 * We assume the input pipe has Tuples with <detailRecordFields> fields that we want
 * to emit. For each group, we'll output the (static) headerRecord Tuple that's provided,
 * plus the detail records, and a trailer record with the detail count filled in. Each
 * record we emit will be prefixed with a "grouping key" field that we use for partitioning.
 * 
 * @author Scale Unlimited
 *
 */
@SuppressWarnings("serial")
public class ReportSubAssembly extends SubAssembly {

    public ReportSubAssembly(Pipe p, Fields groupingFields, TupleEntry headerRecord, TupleEntry detailRecord, Fields detailRecordFields, TupleEntry trailerRecord, String trailerDetailCountFieldname) {
        p = new GroupBy(p, groupingFields);
        p = new Every(p, new GenerateReportHeaderAndTrailer(headerRecord, detailRecord, detailRecordFields, trailerRecord, trailerDetailCountFieldname), Fields.RESULTS);
        
        setTails(p);
    }
    
    private static class GenerateReportHeaderAndTrailer extends BaseOperation<Void> implements Buffer<Void> {
        
        private TupleEntry _headerRecord;
        private TupleEntry _detailRecord;
        private Fields _detailRecordFields;
        private TupleEntry _trailerRecord;
        private String _trailerDetailCountFieldname;
        
        public GenerateReportHeaderAndTrailer(TupleEntry headerRecord, TupleEntry detailRecord, Fields detailRecordFields, TupleEntry trailerRecord, String trailerDetailCountFieldname) {
            super(Fields.UNKNOWN);
            
            
            _headerRecord = headerRecord;
            _detailRecord = detailRecord;
            _detailRecordFields = detailRecordFields;
            // TODO verify that detailRecordFields all exist in detailRecord
            _trailerRecord = trailerRecord;
            _trailerDetailCountFieldname = trailerDetailCountFieldname;
            // TODO verify that _trailerDetailCountFieldname exists in trailerRecord

        }

        @SuppressWarnings("rawtypes")
        @Override
        public void operate(FlowProcess flowProcess, BufferCall<Void> bufferCall) {
            // Build a grouping key, which will be <field 1>-<field 2>, etc.
            TupleEntry groupKey = bufferCall.getGroup();
            StringBuilder group = new StringBuilder();
            for (int i = 0; i < groupKey.size(); i++) {
                if (i > 0) {
                    group.append('-');
                }
                group.append(groupKey.getString(i));
            }
            
            // Assumes the group key value(s) have no tabs in them.
            String groupingKey = group.toString();

            // Output the grouping key, followed by header record fields
            Tuple header = new Tuple(groupingKey);
            header.addAll(_headerRecord.getTuple());
            bufferCall.getOutputCollector().add(header);
            
            Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
            int detailRecordCount = 0;
            
            // iterate over details, adding grouping key to each one at beginning
            while (iter.hasNext()) {
                detailRecordCount += 1;
                _detailRecord.setTuple(_detailRecordFields, iter.next().selectTuple(_detailRecordFields));
                Tuple detail = new Tuple(groupingKey);
                detail.addAll(_detailRecord.getTuple());
                bufferCall.getOutputCollector().add(detail);
            }
            
            // At end, create tuple with grouping key followed by trailer fields, where detail count field is filled in.
            _trailerRecord.setInteger(_trailerDetailCountFieldname, detailRecordCount);
            Tuple trailer = new Tuple(groupingKey);
            trailer.addAll(_trailerRecord.getTuple());
            bufferCall.getOutputCollector().add(trailer);
        }
        
        
    }
}
