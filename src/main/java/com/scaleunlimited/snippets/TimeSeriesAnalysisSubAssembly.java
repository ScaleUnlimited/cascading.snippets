package com.scaleunlimited.snippets;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import com.scaleunlimited.cascading.NullContext;

import cascading.flow.Flow;
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
import cascading.tuple.TupleEntryIterator;

@SuppressWarnings("serial")
public class TimeSeriesAnalysisSubAssembly extends SubAssembly {

    public static String LOCATION_FN = "location";
    public static String EVENT_FN = "event";
    public static String TIME_FN = "time";
    
    // For interesting & significant events
    public static String FIRST_EVENT_FN = "first_event";
    public static String SECOND_EVENT_FN = "second_event";

    // For significant events
    public static String SIGNIFICANT_EVENT_ID_FN = "significant_event_id";
    
    protected static class FindInterestingEvents extends BaseOperation<NullContext> implements Buffer<NullContext> {

        private long eventWindowSize;
        
        public FindInterestingEvents(long eventWindowSize) {
            super(new Fields(LOCATION_FN, FIRST_EVENT_FN, SECOND_EVENT_FN, TIME_FN));
            
            this.eventWindowSize = eventWindowSize;
        }
        
        @Override
        public void operate(FlowProcess flowProcess, BufferCall<NullContext> bufferCall) {
            String location = bufferCall.getGroup().getString(LOCATION_FN);
            
            // We know events are ordered by time. Create a queue that we can use
            Queue<TupleEntry> eventWindow = new LinkedList<TupleEntry>(); 
            
            Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
            while (iter.hasNext()) {
                // Get the next event.
                TupleEntry curEvent = iter.next();
                long eventTime = curEvent.getLong(TIME_FN);
                
                // Flush everything from the front of the queue that's outside the time window for this event.
                long windowStartTime = eventTime - eventWindowSize;
                while (!eventWindow.isEmpty() && (eventWindow.peek().getLong(TIME_FN) < windowStartTime)) {
                    eventWindow.remove();
                }
                
                // Now generate interesting events for everything left in the queue, when combined with the
                // current event.
                Tuple suspiciousEvent = new Tuple(location, "", curEvent.getString(EVENT_FN), curEvent.getLong(TIME_FN));
                for (TupleEntry prevEvent : eventWindow) {
                    // Fill in the previous event's name
                    suspiciousEvent.set(1, prevEvent.getString(EVENT_FN));
                    bufferCall.getOutputCollector().add(suspiciousEvent);
                }
                
                // Now push the current event onto the queue.
                eventWindow.add(new TupleEntry(curEvent));
            }
        }
        
    }

    protected static class FindSignificantEvents extends BaseOperation<NullContext> implements Buffer<NullContext> {

        private long eventWindowSize;
        private int minInterestingEvents;
        
        public FindSignificantEvents(long eventWindowSize, int minInterestingEvents) {
            // first event name, second event name, significant event id, time of second event, 
            super(new Fields(FIRST_EVENT_FN, SECOND_EVENT_FN, SIGNIFICANT_EVENT_ID_FN, TIME_FN));
            
            this.eventWindowSize = eventWindowSize;
            this.minInterestingEvents = minInterestingEvents;
        }
        
        @Override
        public void operate(FlowProcess flowProcess, BufferCall<NullContext> bufferCall) {
            String firstEventName = bufferCall.getGroup().getString(FIRST_EVENT_FN);
            String secondEventName = bufferCall.getGroup().getString(SECOND_EVENT_FN);
            
            // We know events are ordered by time. Create a queue that we can use
            Queue<TupleEntry> eventWindow = new LinkedList<TupleEntry>(); 
            
            // Keep track of whether we're in a window with significant events, as we want
            // to increment the window id only when our window is empty.
            boolean inSignificantWindow = false;
            int windowId = 0;
            
            Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
            while (iter.hasNext()) {
                // Get the next event.
                TupleEntry curEvent = iter.next();
                long eventTime = curEvent.getLong(TIME_FN);
                
                // If there are one or more events at the front of the queue that are outside the time window,
                // flush them now.
                long windowStartTime = eventTime - eventWindowSize;
                while (!eventWindow.isEmpty() && (eventWindow.peek().getLong(TIME_FN) < windowStartTime)) {
                    eventWindow.remove();
                }
                
                // Now generate significant events for everything left in the queue, when combined with the
                // current event, if there are enough of them.
                if (eventWindow.size() + 1 >= minInterestingEvents) {
                    Tuple interestingEvent = new Tuple("", curEvent.getString(EVENT_FN), windowId, curEvent.getLong(TIME_FN));
                    for (TupleEntry prevEvent : eventWindow) {
                        // Fill in the previous event's name
                        interestingEvent.set(0, prevEvent.getString(EVENT_FN));
                        bufferCall.getOutputCollector().add(interestingEvent);
                    }
                } else if (inSignificantWindow) {
                    inSignificantWindow = false;
                    windowId += 1;
                }
                
                // Now push the current event onto the queue.
                eventWindow.add(new TupleEntry(curEvent));
            }
        }
        
    }

    public TimeSeriesAnalysisSubAssembly(Pipe eventPipe, long interestingEventsWindowSize, long significantEventsWindowSize) {
        
        // First we want to group by the location, and sort by the time, so that
        // we can find events that occur within the "interesting" time window.
        // For each such event, we'll emit an (event1, event2) pair, along w/location & time.
       eventPipe = new GroupBy(eventPipe, new Fields(LOCATION_FN), new Fields(TIME_FN));
       Pipe interestingEventsPipe = new Every(eventPipe, new FindInterestingEvents(interestingEventsWindowSize));
       
       // Now we need to group by the two event names, and count the number of occurrences within
       // the significant event time window. If we get enough, then emit as a significant event (with an
       // incrementing number, as we might get more than one window with enough interesting events).
       interestingEventsPipe = new GroupBy(interestingEventsPipe, new Fields(FIRST_EVENT_FN, SECOND_EVENT_FN), new Fields(TIME_FN));
       // Pipe significantEventsPipe = new Every(interestingEventsPipe, new FindSignificantEvents(significantEventsWindowSize));
       
    }
}
