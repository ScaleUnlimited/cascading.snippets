package com.scaleunlimited.snippets;

import java.util.Random;

import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

public class ReplicateTuplesTest {

    @Test
    public void testCartesianJoinWithReplication() throws Exception {

        // Pretend like we know we're running the job on a cluster with N reduce tasks
        final int numReduceTasks = 6;
        
        // To simulate the current requirement, use TextDelimited which sets Fields.UNKNOWN for the source.
        Tap sourceTap = new Lfs(new TextDelimited(), "build/test/ReplicateTuplesTest/testCartesianJoinWithReplication/in");
        TupleEntryCollector write = sourceTap.openForWrite(new HadoopFlowProcess());
        write.add(new Tuple("bob", 1, 10));
        write.add(new Tuple("bob", 1, 50));
        write.add(new Tuple("bob", 6, 60));
        write.add(new Tuple("john", 2, 20));
        write.close();
        
        // Split the pipe into a LHS and a RHS.
        Pipe p = new Pipe("CVS source");
        Pipe lhs = new Pipe("lhs", p);
        Pipe rhs = new Pipe("rhs", p);
        
        // On the LHS, assign each tuple a random grouping value from 0...numReduceTasks - 1.
        // This gets added to the end of the Tuple.
        lhs = new Each(lhs, new RandomFieldFunction(numReduceTasks), Fields.ALL);
        lhs = new Each(lhs, DebugLevel.VERBOSE, new Debug("partitioned", true));
        
        // On the RHS, replicate each tuple numReduceTasks times, using values 0...numReduceTasks - 1
        // for the grouping value. This gets added to the end of each Tuple.
        rhs = new Each(rhs, new ReplicateFunction(numReduceTasks), Fields.ALL);
        rhs = new Each(rhs, DebugLevel.VERBOSE, new Debug("replicated", true));

        // We want to use new Fields(-1) here so we don't need to know the size of the input data, but
        // that doesn't work for some reason.
        Pipe grouped = new CoGroup("grouped", lhs, new Fields(-1), rhs, new Fields(-1));
        
        // At this point you'd do an Every with a custom Buffer. In that class you can calculate your user-user
        // similarity. But note that without field names, you'll be processing a Tuple with N fields, where
        // the first (N - 2)/2 are from the LHS, followed by the left hand grouping key field, followed by
        // (N - 2)/2 fields from the RHS, followed by the right hand grouping key.
        // grouped = new Every(grouped, new UserSimilarity(), Fields.RESULTS);
        
        Tap sinkTap = new Lfs(new TextDelimited(), "build/test/ReplicateTuplesTest/testCartesianJoinWithReplication/out", SinkMode.REPLACE);
        Flow flow = new HadoopFlowConnector().connect(sourceTap, sinkTap, grouped);
        flow.complete();
    }

    private static class ReplicateFunction extends BaseOperation<Void> implements Function<Void> {
        
        private int _numCopies;
        
        public ReplicateFunction(int numCopies) {
            super(new Fields("group"));
            
            _numCopies = numCopies;
        }
        
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<Void> functionCall) {
            for (int i = 0; i < _numCopies; i++) {
                functionCall.getOutputCollector().add(new Tuple(i));
            }
        }
    }
    
    private static class RandomFieldFunction extends BaseOperation<Void> implements Function<Void> {
        
        private int _limit;
        
        private transient Random _rand;
        
        public RandomFieldFunction(int limit) {
            super(new Fields("partition"));
            
            _limit = limit;
        }
        
        @Override
        public void prepare(FlowProcess flowProcess, OperationCall<Void> operationCall) {
            super.prepare(flowProcess, operationCall);
            
            _rand = new Random();
        }
        
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<Void> functionCall) {
            functionCall.getOutputCollector().add(new Tuple(_rand.nextInt(_limit)));
        }
    }
}
