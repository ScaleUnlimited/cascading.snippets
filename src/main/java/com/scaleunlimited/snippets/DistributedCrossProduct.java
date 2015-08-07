package com.scaleunlimited.snippets;

import java.util.Random;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Retain;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.scaleunlimited.cascading.NullContext;

@SuppressWarnings("serial")
public class DistributedCrossProduct extends SubAssembly {

    private static final Fields LHS_GROUP_FIELD = new Fields(DistributedCrossProduct.class.getSimpleName() + "_lhsgroup");
    private static final Fields RHS_GROUP_FIELD = new Fields(DistributedCrossProduct.class.getSimpleName() + "_rhsgroup");
    
    /**
     * Do a self-join distributed cross product (cartesian join) on a single pipe.
     * 
     * @param p
     * @param numReducers
     */
    public DistributedCrossProduct(Pipe p, int numReducers) {
        
        Pipe lhs = new Pipe("DistributedCrossProduct-lhs", p);
        lhs = new Each(lhs, new InsertRandom(numReducers), Fields.ALL);
        lhs = new Each(lhs, new RemoveFieldnames());
        
        Pipe rhs = new Pipe("DistributedCrossProduct-rhs", p);
        rhs = new Each(rhs, new Replicate(numReducers), Fields.ALL);
        rhs = new Each(rhs, new RemoveFieldnames());

        // Do the cross-join using the last field (the one we just added) in each pipe.
        Pipe joined = new CoGroup(lhs, new Fields(-1), rhs, new Fields(-1));
        
        setTails(joined);
    }

    public DistributedCrossProduct(Pipe lhs, Pipe rhs, int numReducers, boolean removeGroupFields) {
        lhs = new Each(lhs, new InsertRandom(numReducers), Fields.ALL);
        rhs = new Each(rhs, new Replicate(numReducers), Fields.ALL);
        
        Pipe joined = new CoGroup(lhs, LHS_GROUP_FIELD, rhs, RHS_GROUP_FIELD);
        
        if (removeGroupFields) {
            // Get rid of the extra fields we added.
            joined = new Discard(joined, LHS_GROUP_FIELD.append(RHS_GROUP_FIELD));
        }
        
        setTails(joined);
    }
    
    public DistributedCrossProduct(Pipe lhs, Fields lhsFields, Pipe rhs, Fields rhsFields, int numReducers) {
        lhs = new Each(lhs, new InsertRandom(numReducers), Fields.ALL);
        rhs = new Each(rhs, new Replicate(numReducers), Fields.ALL);
        
        Pipe joined = new CoGroup(lhs, LHS_GROUP_FIELD, rhs, RHS_GROUP_FIELD);
        
        // Get rid of temp fields used for grouping
        joined = new Retain(joined, lhsFields.append(rhsFields));
        setTails(joined);
    }
    
    public static class InsertRandom extends BaseOperation<NullContext> implements Function<NullContext> {
        
        private int _range;
        private transient Random _rand;
        private transient Tuple _result;
        
        public InsertRandom(int range) {
            super(LHS_GROUP_FIELD);
            
            _range = range;
        }
        
        @Override
        public void prepare(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
            super.prepare(flowProcess, operationCall);
            
            _rand = new Random();
            _result = Tuple.size(1);
        }
        
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
            _result.set(0, _rand.nextInt(_range));
            functionCall.getOutputCollector().add(_result);
        }
    }
    
    public static class Replicate extends BaseOperation<NullContext> implements Function<NullContext> {
        
        private int _numReplicas;
        private transient Tuple _result;
        
        public Replicate(int range) {
            super(RHS_GROUP_FIELD);
            
            _numReplicas = range;
        }
        
        @Override
        public void prepare(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
            super.prepare(flowProcess, operationCall);
            
            _result = Tuple.size(1);
        }
        
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
            for (int i = 0; i < _numReplicas; i++) {
                _result.set(0, i);
                functionCall.getOutputCollector().add(_result);
            }
        }
    }
    
    private static class RemoveFieldnames extends BaseOperation<Void> implements Function<Void> {
        
        public RemoveFieldnames() {
            super(Fields.UNKNOWN);
        }
        
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<Void> functionCall) {
            functionCall.getOutputCollector().add(functionCall.getArguments().getTuple());
        }
    }
    

}
