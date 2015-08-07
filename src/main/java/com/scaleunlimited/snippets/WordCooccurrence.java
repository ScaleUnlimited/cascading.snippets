package com.scaleunlimited.snippets;

import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.Identity;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Unique;
import cascading.pipe.joiner.InnerJoin;
import cascading.tuple.Fields;

import com.scaleunlimited.cascading.NullContext;

@SuppressWarnings("serial")
public class WordCooccurrence extends SubAssembly {

    private static class SetFilter extends BaseOperation<NullContext> implements Filter<NullContext> {

        private Set<String> _validWords;
        
        public SetFilter(Set<String> validWords) {
            _validWords = validWords;
        }
        
        @SuppressWarnings("rawtypes")
        @Override
        public boolean isRemove(FlowProcess flowProcess, FilterCall<NullContext> filterCall) {
            return !_validWords.contains(filterCall.getArguments().getTuple().getString(1));
        }

    }
    
    public WordCooccurrence(Pipe documentsPipe, String docIdFieldname, String docTextFieldname, Set<String> listA, Set<String> listB) {
        
        // Filter out everything but what we need.
        documentsPipe = new Each(documentsPipe, new Fields(docIdFieldname, docTextFieldname), new Identity());

        // Parse the text into separate words. We could do this more efficiently by implementing a custom function that
        // outputs <doc id> and <word> as two fields.
        //
        // Normally you'd do something better here for parsing, and you'd want to normalize text
        documentsPipe = new Each(documentsPipe, new Fields(docTextFieldname), new RegexSplitGenerator(new Fields("listAword"), "\\s+"), Fields.ALL);
        
        // Filter out the original docTextFieldname.
        documentsPipe = new Each(documentsPipe, new Fields(docIdFieldname, "listAword"), new Identity());

        // We only care about unique terms.
        documentsPipe = new Unique(documentsPipe, new Fields(docIdFieldname, "listAword"));
        
        // Split, and only leave around words that exist in either set
        Pipe listAPipe = new Pipe("list A words", documentsPipe);
        listAPipe = new Each(listAPipe, new SetFilter(listA));

        Pipe listBPipe = new Pipe("list B words", documentsPipe);
        listBPipe = new Each(listBPipe, new SetFilter(listB));

        // Rename this right side to avoid field name collisions in the CoGroup
        listBPipe = new Rename(listBPipe, new Fields(docIdFieldname, "listAword"), new Fields("listBdocID", "listBword"));

        // Do a CoGroup with an outer join to get all of the combinations.
        Pipe resultsPipe = new CoGroup( listAPipe, new Fields(docIdFieldname),
                                        listBPipe, new Fields("listBdocID"),
                                        null,
                                        new InnerJoin());

        // Get rid of the extra listBdocID field that we don't need
        resultsPipe = new Each(resultsPipe, new Fields(docIdFieldname, "listAword", "listBword"), new Identity());

        setTails(resultsPipe);
    }
}
