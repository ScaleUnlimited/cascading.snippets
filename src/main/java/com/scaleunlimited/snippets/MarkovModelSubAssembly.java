package com.scaleunlimited.snippets;

import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.aggregator.First;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.filter.Limit;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.Discard;
import cascading.pipe.joiner.RightJoin;
import cascading.tuple.Fields;

/**
 * We get passed a Tuple that has two objects in it, which we treat as the bigram.
 * We'll output the top N probabilities (per leading value) for the top M (by count) leading
 * values.
 *
 */
@SuppressWarnings("serial")
public class MarkovModelSubAssembly extends SubAssembly {

    // Default name for output field
    public static final String BIGRAM_PROBABILITY_FN = "bigram-probability";

    // Q: what if you want to carry field names through the process?
    // Q: what if you want to control threshold for aggregators?
    // Q: what if you want to indicate that unique leading values is small enough
    // to fit in memory, so use a HashJoin?
    // Q: could you make this more efficient if you knew that the maximum number of
    // trailing values for any one leading value could fit in memory, and thus you
    // could use a custom Buffer to calculate probabilities and limit the number of
    // leading values to the top N?
    
    private static final int MAX_LEADING_BIGRAMS = 1000;
    private static final int MAX_PROBABILITIES_PER_BIGRAM = 1000;

    private static final String LEADING_VALUE_COUNT_FN = "markovmodel_leading_count";
    private static final String BIGRAM_COUNT_FN = "markovmodel_bigram_count";
    
    public MarkovModelSubAssembly(Pipe sourcePipe) {
        this(sourcePipe, new Fields(BIGRAM_PROBABILITY_FN));
    }
    
    public MarkovModelSubAssembly(Pipe sourcePipe, Fields resultField) {
        this(sourcePipe, resultField, MAX_LEADING_BIGRAMS, MAX_PROBABILITIES_PER_BIGRAM);
    }

    public MarkovModelSubAssembly(Pipe sourcePipe, Fields resultField, int maxLeadingValues, int maxResultsPerLeadingValue) {
        // Verify we get one regular field passed in
        if (!resultField.isDefined() || (resultField.size() != 1)) {
            throw new IllegalArgumentException("resultField must be one regular field");
        }
        
        // We need to get a total count of tuples which have the same leading value, so we
        // can calculate probabilities for the second value.
        Pipe leadingValueCountPipe = new Pipe("leading value counts", sourcePipe);
        leadingValueCountPipe = new CountBy("count bigrams per leading value", leadingValueCountPipe, new Fields(0), new Fields(LEADING_VALUE_COUNT_FN));
        leadingValueCountPipe = new Each(leadingValueCountPipe, DebugLevel.VERBOSE, new Debug("leading value counts", true));
        // leadingValueCountPipe now has [leading value, leading value count]
        
        // If the user has specified a max leading bigram, we want the N bigrams that occur
        // most often.
        if (maxLeadingValues != Integer.MAX_VALUE) {
            // Note that Limit() is approximate, since this will be getting applied separately
            // in each reducer. Cascading estimates the limit to use, but it might not give
            // us the actual best tuples, based on how these get distributed (by hash of the
            // count field) to the reducers.
            // the post-reducer set of Tuples on each 
            leadingValueCountPipe = new GroupBy("limit leading values", leadingValueCountPipe, new Fields(LEADING_VALUE_COUNT_FN), true);
            leadingValueCountPipe = new Each(leadingValueCountPipe, new Limit(maxLeadingValues));
            leadingValueCountPipe = new Each(leadingValueCountPipe, DebugLevel.VERBOSE, new Debug("leading values limited", true));
        }
        
        // We need to count occurrences of each bigram combination too.
        Pipe bigramCountPipe = new Pipe("bigram counts", sourcePipe);
        bigramCountPipe = new CountBy("count bigrams", bigramCountPipe, new Fields(0, 1), new Fields(BIGRAM_COUNT_FN));
        bigramCountPipe = new Each(bigramCountPipe, DebugLevel.VERBOSE, new Debug("bigram counts", true));
        // bigramCountPipe now has [leading value, trailing value, bigram count]
        
        // If we join these two together, we'll have the data we need.
        // Q: Why do we put the leadingValueCountPipe on the right side (second pipe)?
        // Q: Why do we use a RightJoin?
        Pipe resultPipe = new CoGroup(  "join leading & bigram counts",
                                        bigramCountPipe,
                                        new Fields(0),
                                        leadingValueCountPipe,
                                        new Fields(0),
                                        null,
                                        new RightJoin());
        resultPipe = new Each(resultPipe, DebugLevel.VERBOSE, new Debug("join leading & bigram counts", true));
        // resultPipe now has [leading value, trailing value, bigram count, leading value, leading value count]
        
        // Calculate each bigram's probability.
        resultPipe = new Each(  resultPipe,
                                new Fields(BIGRAM_COUNT_FN, LEADING_VALUE_COUNT_FN),
                                new ExpressionFunction(resultField, "$0 / $1", Float.class),
                                Fields.SWAP);
        resultPipe = new Each(resultPipe, DebugLevel.VERBOSE, new Debug("probability calculated", true));
        // resultPipe now has [leading value, trailing value, leading value, bigram probability]
        
        // We want to trim down the Tuple, since we've got an extra leading value in it at position 3
        resultPipe = new Discard(resultPipe, new Fields(2));
        resultPipe = new Each(resultPipe, DebugLevel.VERBOSE, new Debug("extra field discarded", true));
        // resultPipe now has [leading value, trailing value, bigram probability]

        // If the user wants some maximum number of probabilities per leading value, let's trim it now
        if (maxResultsPerLeadingValue != Integer.MAX_VALUE) {
            // Group by leading value, sort by probability (high to low), and take the first N values.
            resultPipe = new GroupBy("sort probabilities", resultPipe, new Fields(0), resultField, true);
            resultPipe = new Every(resultPipe, new First(maxResultsPerLeadingValue), Fields.RESULTS);
            resultPipe = new Each(resultPipe, DebugLevel.VERBOSE, new Debug("results per leading value limited", true));
        }
        
        setTails(resultPipe);

    }

}
