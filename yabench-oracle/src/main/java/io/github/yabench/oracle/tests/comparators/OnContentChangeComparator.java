package io.github.yabench.oracle.tests.comparators;
import io.github.yabench.oracle.*;
import io.github.yabench.oracle.BindingWindow;
import io.github.yabench.oracle.OracleResult;
import io.github.yabench.oracle.readers.EngineResultsReader;
import io.github.yabench.oracle.readers.BufferedTWReader;
import io.github.yabench.oracle.OracleResultBuilder;
import io.github.yabench.oracle.OracleResultsWriter;
import io.github.yabench.oracle.QueryExecutor;
import io.github.yabench.oracle.TripleWindow;
import io.github.yabench.oracle.Window;
import io.github.yabench.oracle.WindowFactory;
import io.github.yabench.oracle.WindowUtils;
import io.github.yabench.oracle.readers.BufferedERReader;
import io.github.yabench.oracle.readers.TripleWindowReader;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.core.Var;
import java.io.*;
import java.util.Iterator;

public class OnContentChangeComparator implements OracleComparator {

    private static final Logger logger = LoggerFactory.getLogger(
            OnContentChangeComparator.class);
    private final BufferedTWReader isReader;
    private final BufferedERReader qrReader;
    private final WindowFactory windowFactory;
    private final QueryExecutor qexec;
    private final OCCORWriter orWriter;
    private final boolean graceful;
    private final boolean singleResult;
    private Writer writer = null;
    private Writer actual_writer = null;

    OnContentChangeComparator(TripleWindowReader inputStreamReader,
            EngineResultsReader queryResultsReader,
            WindowFactory windowFactory, QueryExecutor queryExecutor,
            OracleResultsWriter oracleResultsWriter, boolean graceful,
            boolean singleResult) {
        this.isReader = new BufferedTWReader(inputStreamReader);
        this.qrReader = new BufferedERReader(queryResultsReader);
        this.windowFactory = windowFactory;
        this.qexec = queryExecutor;
        this.orWriter = new OCCORWriter(isReader, oracleResultsWriter,
                windowFactory.getSlide().toMillis(),
                windowFactory.getSize().toMillis());
        this.graceful = graceful;
        this.singleResult = singleResult;
        try{
        	this.writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("oracle_result.txt"), "utf-8"));
                this.actual_writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("actual_result.txt"), "utf-8"));
        }catch(Exception ex){

        }
    }

    @Override
    public void compare() throws IOException {
        if (graceful) {
            compareInGraceful();
        } else {
            newcompare();
            //compareInNonGraceful();
        }
    }

    public void newcompare() throws IOException {
        final OracleResultBuilder oracleResultBuilder = new OracleResultBuilder();
        for (int i = 1;; i++) {
            final Window window = windowFactory.nextWindow();
            final BindingWindow actual = qrReader.next();
            if (actual != null) {
                for(Binding b: actual.getBindings()){
                    Iterator<Var> it = b.vars();
                    while(it.hasNext()){
                        actual_writer.write(b.get(it.next()).toString()+'\t');
                    }
                    actual_writer.write('\n');
                }
                actual_writer.flush();
                isReader.purge(window.getStart());
                final TripleWindow inputWindow = isReader.readNextWindow(window);
                //logger.info("-----------------window: "+inputWindow.getTriples().size());
                if (inputWindow.getTriples().size()!=0) {
                    
                    BindingWindow expected = qexec.executeSelect(inputWindow);
                    //logger.info("expected: "+expected.getBindings().size());
                    //writer.write("start");

                    for(Binding b: expected.getBindings()){
                        Iterator<Var> it = b.vars();
                        while(it.hasNext()){
                            writer.write(b.get(it.next()).toString()+'\t');
                        }
                        writer.write('\n');
                    }
                    writer.flush();
                    final FMeasure fMeasure = new FMeasure().calculateScores(expected.getBindings(), actual.getBindings());
                    FMeasure prevfMeasure = fMeasure;

                    if (!prevfMeasure.getNotFoundReferences().isEmpty()) {
                        logger.info("Window #{} [{}:{}]. Missing triples:", i, window.getStart(), window.getEnd());
                        logger.info("missing triples");
                    }

                    long startshift = (i * windowFactory.getSlide().toMillis()) - windowFactory.getSize().toMillis();
                    long endshift = (i * windowFactory.getSlide().toMillis());

                    //logger.info("expected bindings size: " + String.valueOf(expected.getBindings().size()));
                    //logger.info("actual bindings size: " + String.valueOf(actual.getBindings().size()));
                    //logger.info("precision: " + prevfMeasure.getPrecisionScore());
                    //logger.info("recall: " + prevfMeasure.getRecallScore());
                    //logger.info("wsize: " + inputWindow.getTriples().size());
                    long delay = actual.getEnd() - expected.getEnd();

                    if (!(expected.getBindings().size() == 0 && actual.getBindings().size() == 0)) {

                    } else {
                        logger.info("0 bindings!");
                    }

                    // todo: what if triple in the middle is missing? not
                    // covered currently, because we assume that only triples at
                    // window borders (start/end) can be missing

                    if (!prevfMeasure.getNotFoundReferences().isEmpty()) {
                        // logger.info("Window #{} [{}:{}]. Missing triples in loop:\n{}",
                        // i, ts, this.windowFactory.getSize()
                        // .toMillis(),
                        // newfMeasure.getNotFoundReferences());
                        logger.info("missing triples!");
                    }

                    startshift = startshift < 0 ? 0 : startshift;

                    orWriter.write(oracleResultBuilder.fMeasure(prevfMeasure).resultSize(expected, actual)
                            .expectedInputSize(inputWindow.getTriples().size()).startshift(startshift).endshift(endshift).delay(delay).build());
                    
                } else {
//                    throw new IllegalStateException("Actual results have more windows than expected!");
                }
            } else {
                break;
            }
        }
        writer.close();
        actual_writer.close();

    }    

    public void compareInGraceful() throws IOException {
        BindingWindow current = null;
        BindingWindow previous = null;
        int actualIndex = -1;
        do {
            final long nextTime = isReader.readTimestampOfNextTriple();
            if (nextTime >= 0) {
                Window window = windowFactory.nextWindow(nextTime, 1);
                TripleWindow inputWindow = isReader.readNextWindow(window);
                if (actualIndex < 0) {
                    actualIndex = qrReader.nextIndex();
                }
                if (current != null && !current.isEmpty()) {
                    previous = current;
                }
//                logger.debug("New previous: {}", previous);
                boolean previousMatched = false;
                boolean tryPreviousWindow = false;
                TripleWindow previousWindow = null;
                do {
//                    logger.debug("Input: {}", inputWindow);
                    current = qexec.executeSelect(inputWindow);
                    if (!current.isEmpty()) {
//                        logger.debug("Current: {}", current);
                        BindingWindow currentReduced;
                        if (previous != null) {
                            currentReduced = WindowUtils.diff(current, previous);
                        } else {
                            currentReduced = current;
                        }
                        if (currentReduced != null && !currentReduced.isEmpty()) {
                            final List<BindingWindow> expected = currentReduced.split();
//                            logger.debug("Previous: {}", previous);
//                            logger.debug("Expected: {}", expected);
                            int j = actualIndex;
                            int actualSize = 0;
                            BindingWindow actual = qrReader.getOrNext(j++);
//                            logger.debug("Actual: {}", actual);
                            if (actual != null) {
                                actualSize += actual.getBindings().size();
                                if (WindowUtils.match(actual, expected)) {
                                    final int numberOfResults = expected.size();
                                    for (int i = 0; i < numberOfResults; i++) {
                                        final BindingWindow found = WindowUtils.findMatch(actual, expected);
                                        if (found != null) {
                                            expected.remove(found);
//                                            logger.debug("Found: {}", found);
                                            actualSize += actual.getBindings().size();

                                            if (!expected.isEmpty()) {
                                                actual = qrReader.getOrNext(j++);
						
//                                                logger.debug("Next actual: {}", actual);
                                                if (actual == null) {
                                                    break;
                                                }
                                            }
                                        } else {
                                            if (tryPreviousWindow) {
                                                //logger.debug("Missing triples: {}", expected);
                                                orWriter.writeMissing(
                                                        inputWindow, expected, actual);

                                                expected.clear();
                                                j--; //get back one actual result
                                            }
                                            break;
                                        }
                                    }
                                    previousMatched = true;
                                    previousWindow = inputWindow;
                                } else {
                                    if (previousMatched) {
//                                        logger.debug("Current window doesn't match, "
//                                                + "but previous did, so let's switch back!");
                                        tryPreviousWindow = true;
                                    }
                                    previousMatched = false;
                                }
                                if (!expected.isEmpty()) {
                                    if (tryPreviousWindow) {
                                        inputWindow = previousWindow;
                                    } else {
//                                        logger.debug("Not equal, reducing and trying once more!");
                                        inputWindow = isReader
                                                .readWindowWithoutFirstGraph(inputWindow);
                                    }
                                } else {
                                    orWriter.writeFound(current, inputWindow, actualSize);
                                    actualIndex = j;

                                    break;
                                }
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                } while (true);
            } else {
                BindingWindow actual = qrReader.getOrNext(actualIndex++);
                if (actual != null) {
                    logger.warn("There are still actual results!");
                    do {
                        orWriter.writeMissingExpected(actual);
                        //logger.debug("{}", actual);
                    } while ((actual = qrReader.getOrNext(actualIndex++)) != null);
                }
                break;
            }
        } while (true);

        orWriter.flush();
    }

    public void compareInNonGraceful() throws IOException {
        BindingWindow previous = null;
        BindingWindow actual = null;
        do {
            final long nextTime = isReader.readTimestampOfNextTriple();
            //logger.info("time:"+nextTime);
	    if (nextTime >= 0) {
                final Window window = windowFactory.nextWindow(nextTime);
                final TripleWindow inputWindow = isReader.readNextWindow(window);
                //logger.info("inputWindow: {}", inputWindow);
		final BindingWindow current = qexec.executeSelect(inputWindow);
		//logger.debug("-------------------------size:"+current.getBindings().size()+"----------------------");
                //logger.debug("oracle: {}", current);
		if (!current.isEmpty()) {
                    List<BindingWindow> expected = WindowUtils
                            .diff(current, previous).split();
		    //logger.debug("--------------------diffsize:"+expected.size()+"-------------------------");
                    //logger.debug("expected:");
                    //logger.debug("{}", expected);
                    //logger.debug("acutal:");
                    //logger.debug("{}", actual);

                    for(BindingWindow bw: expected){
			for(Binding b : bw.getBindings()){
                            Iterator<Var> it = b.vars();
                            while(it.hasNext()){
                                writer.write(b.get(it.next()).toString()+'\t');
                            }
                            writer.write('\n');
			}
                    }
                    writer.flush();

		    if (!expected.isEmpty()) {
                        //logger.debug("Expected: {}", expected);

                        if (actual == null) {
                            actual = qrReader.next();
 			    if(actual != null){
                            	for(Binding b: actual.getBindings()){
                                	Iterator<Var> it = b.vars();
                                	while(it.hasNext()){
                                        	actual_writer.write(b.get(it.next()).toString()+'\t');
                                	}
                                	actual_writer.write('\n');
                            	}
			    	actual_writer.flush();
			    }
	                }
                        //logger.debug("Actual: {}", actual);

                        if (actual != null) {
                            FindResult result = find(inputWindow, expected, actual);
                            actual = result.actual;

                            if(actual != null && !result.matched) {
                                if (singleResult) {
                                    //actual = null;
                                    do {
                                        if((actual = qrReader.next()) != null) {
                        		
					    for(Binding b: actual.getBindings()){
                                		Iterator<Var> it = b.vars();
                                		while(it.hasNext()){
                                        		actual_writer.write(b.get(it.next()).toString()+'\t');
                                		}
                                		actual_writer.write('\n');
                        		    }
					    actual_writer.flush();
			                
					    //logger.debug("Actual (S): {}", actual);
                                            result = find(inputWindow, expected, actual);
                                            actual = result.actual;
                                        }
                                    } while (actual != null && !result.matched);
                                } else {
                                    do {
					if((actual = qrReader.next()) != null) {
	                                    for(Binding b: actual.getBindings()){
        	                        	Iterator<Var> it = b.vars();
                	                        while(it.hasNext()){
                        	        		actual_writer.write(b.get(it.next()).toString()+'\t');
                                	        }
                                        	actual_writer.write('\n');
                                       	    }
					    //logger.debug("Actual (S): {}", actual);
                                            result = find(inputWindow, expected, actual);
                                            actual = result.actual;
                                        }
                                    } while (actual != null && !result.matched);
                                }
                            }
                        } else {
                            //logger.debug(
                            //        "Expected, but there are no actual results anymore: {}",
                            //        expected);
                            orWriter.writeMissingActual(expected);
                        }

                        previous = current;
                    }
                }
                isReader.purge(window.getStart() 
                        - windowFactory.getWindowSize().toMillis());
            } else {
                if ((actual = qrReader.next()) != null) {
                    //logger.debug("Didn't find expected for:");
                    do {
                       for(Binding b: actual.getBindings()){
                                Iterator<Var> it = b.vars();
                                while(it.hasNext()){
                                        actual_writer.write(b.get(it.next()).toString()+'\t');
                                }
                                actual_writer.write('\n');
                        }
                        actual_writer.flush();

                        //logger.debug("NOT INPUT: {}", actual);
                        orWriter.writeMissingExpected(actual);
                    } while ((actual = qrReader.next()) != null);
                }

                break;
            }
        } while (true);
	writer.close();
	actual_writer.close();
        orWriter.flush();
    }

    private FindResult find(TripleWindow inputWindow,
            List<BindingWindow> results, BindingWindow actual)
            throws IOException {
        FindResult result = new FindResult();
        final int numberOfResults = results.size();
        for (int i = 0; i < numberOfResults; i++) {
            final BindingWindow found = WindowUtils.findMatch(actual, results);
            if (found != null) {
                result.matched = true;
                //logger.debug("Found: {}", found);

                results.remove(found);

                orWriter.writeFound(inputWindow, found, actual);

                if (!results.isEmpty() && (actual = qrReader.next()) == null) {
                        
			for(Binding b: actual.getBindings()){
                                Iterator<Var> it = b.vars();
                                while(it.hasNext()){
                                        actual_writer.write(b.get(it.next()).toString()+'\t');
                                }
                                actual_writer.write('\n');
                        }
			actual_writer.flush();
			
		    //logger.info("actual 2: {}", actual);
		    logger.warn(
                            "Expected, but there are no actual results anymore: {}",
                            results);

                    orWriter.writeMissingActual(results);
                    return result;
                }
		if (!results.isEmpty() && actual!=null) {
                        for(Binding b: actual.getBindings()){
                                Iterator<Var> it = b.vars();
                                while(it.hasNext()){
                                        actual_writer.write(b.get(it.next()).toString()+'\t');
                                }
                                actual_writer.write('\n');
                        }
                        actual_writer.flush();
		}

            } else {
                //logger.debug("Expected, but haven't found: {}", results);
                //logger.debug("It was: {}", actual);
                orWriter.writeMissing(inputWindow, results, actual);

                result.actual = actual;
                return result;
            }
        }

        return result;
    }

    public static class FindResult {

        public BindingWindow actual = null;
        public boolean matched = false;
    }

    public static class OCCORWriter extends OracleResultsWriter {

        public static final double ONE = 1.0;
        public static final double ZERO = 0.0;
        private final BufferedTWReader erReader;
        private final List<OracleResult> results = new ArrayList<>();
        private final long windowSize;
        private final long windowSlide;
        private int currentWindowNumber = 0;
        private boolean isFirstWindow = true;

        public OCCORWriter(BufferedTWReader erReader, 
                OracleResultsWriter writer, long windowSlide, long windowSize) {
            this(erReader, writer.getWriter(), windowSlide, windowSize);
        }

        public OCCORWriter(BufferedTWReader erReader, Writer writer, long windowSlide, long windowSize) {
            super(writer);
            this.erReader = erReader;
            this.windowSlide = windowSlide;
            this.windowSize = windowSize;
        }

        public void writeMissingActual(List<BindingWindow> expected) throws IOException {
            for (BindingWindow e : expected) {
                writeMissingActual(e);
            }
        }

        public void writeMissingActual(BindingWindow expected) throws IOException {
            final OracleResultBuilder builder = new OracleResultBuilder();
	    //logger.info("###################missing actual#######################");
            write(builder
                    .precision(ONE)
                    .recall(ZERO)
                    .startshift(expected.getStart())
                    .endshift(expected.getEnd())
                    .expectedResultSize(expected.getBindings().size())
                    .delay(-1)
                    .build());
        }

        public void writeMissingExpected(BindingWindow actual) throws IOException {
            int numberOfSlides = 0;
            while ((numberOfSlides + 1) * windowSlide < actual.getEnd()) {
                numberOfSlides++;
            }
	    //logger.info("^^^^^^^^^^^^^^^^^missing expected^^^^^^^^^^^^^^^^^^^^");
            final OracleResultBuilder builder = new OracleResultBuilder();
            write(builder
                    .precision(ZERO)
                    .recall(ONE)
                    .startshift(numberOfSlides * windowSlide)
                    .endshift(actual.getEnd())
                    .actualResultSize(actual.getBindings().size())
                    .delay(-1)
                    .build());
        }

        public void writeMissing(TripleWindow inputWindow,
                List<BindingWindow> expected, BindingWindow actual)
                throws IOException {
            writeMissing(inputWindow, WindowUtils.merge(expected), actual);
        }

        public void writeMissing(TripleWindow inputWindow,
                BindingWindow expected, BindingWindow actual)
                throws IOException {
		//logger.info("*****************missing************");
            final OracleResultBuilder builder = new OracleResultBuilder();
            write(builder
                    .precision(ZERO)
                    .recall(ZERO)
                    .startshift(expected.getStart())
                    .endshift(expected.getEnd())
                    .actualResultSize(actual.getBindings().size())
                    .expectedResultSize(expected.getBindings().size())
                    .expectedInputSize(inputWindow.getTriples().size())
                    .delay(-1)
                    .build());
        }

        public void writeFound(BindingWindow expected, TripleWindow input,
                int actualResults) throws IOException {
            final OracleResultBuilder builder = new OracleResultBuilder();
		//logger.info("------------------found--------------");
            write(builder
                    .precision(ONE)
                    .recall(ONE)
                    .startshift(expected.getStart())
                    .endshift(expected.getEnd())
                    .actualResultSize(actualResults)
                    .expectedResultSize(expected.getBindings().size())
                    .expectedInputSize(input.getTriples().size())
                    .build());
        }

        public void writeFound(TripleWindow inputWindow,
                BindingWindow expected, BindingWindow actual)
                throws IOException {
		//logger.info("-----------------found-----------------");
            final OracleResultBuilder builder = new OracleResultBuilder();
            write(builder
                    .precision(ONE)
                    .recall(ONE)
                    .startshift(expected.getStart())
                    .endshift(expected.getEnd())
                    .actualResultSize(actual.getBindings().size())
                    .expectedResultSize(expected.getBindings().size())
                    .expectedInputSize(inputWindow.getTriples().size())
                    .delay(actual.getEnd() - expected.getEnd())
                    .build());
        }

        @Override
        public void write(OracleResult result) throws IOException {
            int numberOfSlides = 0;
            while ((numberOfSlides + 1) * windowSlide < result.getEndshift()) {
                numberOfSlides++;
            }

            if (numberOfSlides == currentWindowNumber || isFirstWindow) {
                results.add(result);

                writeEmptyWindows(currentWindowNumber, numberOfSlides);

                currentWindowNumber = numberOfSlides;
                isFirstWindow = false;
            } else {
                if (!results.isEmpty()) {
                    super.write(merge(results));
                }

                if (numberOfSlides - currentWindowNumber > 1) {
                    writeEmptyWindows(currentWindowNumber + 1, numberOfSlides);
                }

                //Clean the buffer to collect results for the next window
                results.clear();
                results.add(result);
                currentWindowNumber = numberOfSlides;
            }
        }

        private void writeEmptyWindows(int current, int slides)
                throws IOException {
            final OracleResultBuilder builder = new OracleResultBuilder();
            for (int i = current; i < slides; i++) {
                //logger.info("++++++++++++++++empty+++++++++++++++++");
		super.write(builder
                        .precision(ONE)
                        .recall(ONE)
                        .startshift(i * windowSlide)
                        .endshift(i * windowSlide + windowSize)
                        .expectedInputSize(0)
                        .expectedResultSize(0)
                        .actualResultSize(0)
                        .delay(0)
                        .build());
            }
        }

        private OracleResult merge(List<OracleResult> rs) {
            double precision = 0;
            double recall = 0;
            int actualRS = 0;
            int expectedRS = 0;
            int delayNum = 0;
            long delay = 0;
            long windowStart = Long.MAX_VALUE;
            long windowEnd = Long.MIN_VALUE;

            for (OracleResult r : rs) {
                precision += r.getPrecision();
                recall += r.getRecall();
                actualRS += r.getActualResultSize();
                //logger.info("expected result r: "+r.getExpectedResultSize());

                expectedRS += r.getExpectedResultSize();
                delay += r.getDelay() > 0 ? r.getDelay() : 0;
                delayNum += r.getDelay() > 0 ? 1 : 0;
                if (r.getStartshift() < windowStart) {
                    windowStart = r.getStartshift();
                }
                if (r.getEndshift() > windowEnd) {
                    windowEnd = r.getEndshift();
                }
            }

            int divisor = rs.isEmpty() ? 1 : rs.size();
            delayNum = delayNum == 0 ? 1 : delayNum;

            final OracleResultBuilder builder = new OracleResultBuilder();
            final OracleResult windowResult = builder
                    .precision(precision / divisor)
                    .recall(recall / divisor)
                    .delay(delay / delayNum)
                    .startshift(windowStart)
                    .endshift(windowEnd)
                    .expectedResultSize(expectedRS)
                    .expectedInputSize(erReader.sizeOfGraph(
                            windowStart, windowStart + windowSize))
                    .actualResultSize(actualRS)
                    .build();
/*
            logger.info("result.size: "+results.size());            
            logger.info("expected bindings size: " + String.valueOf(expectedRS));
            logger.info("actual bindings size: " + String.valueOf(actualRS));
            logger.info("precision: " + precision/divisor);
            logger.info("recall: " + recall/divisor);
            logger.info("wsize: " + erReader.sizeOfGraph(
                    windowStart, windowStart + windowSize));
*/
            return windowResult;
        }

        public void flush() throws IOException {
	    final OracleResult windowResult = merge(results);
            super.write(windowResult);
        }

    }
}
