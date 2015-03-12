package io.github.yabench.oracle;

import io.github.yabench.commons.RDFStreamReader;
import io.github.yabench.commons.TemporalTriple;
import java.io.IOException;
import java.io.Reader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WindowFactory {

    private final RDFStreamReader reader;
    private final Duration windowSize;
    private final Duration windowSlide;
    private long numberOfSlides = 0;
    private List<TemporalTriple> content = new ArrayList<>();

    public WindowFactory(Reader reader, Duration windowSize, Duration windowSlide)
            throws IOException {
        this.reader = new RDFStreamReader(reader);
        this.windowSize = windowSize;
        this.windowSlide = windowSlide;
    }

    /**
     * @return null if the end of the stream has been reached
     * @throws IOException 
     */
    public TripleWindow nextWindow() throws IOException {
        numberOfSlides++;
        
        final long windowEnd = numberOfSlides * windowSlide.toMillis();
        final long windowStart = 
                windowEnd - windowSize.toMillis() > 0 ? windowEnd - windowSize.toMillis() : 0;
        
        content = new ArrayList<>(Arrays.asList(content.stream()
                .filter((triple)-> triple.getTime() >= windowStart)
                .toArray(TemporalTriple[]::new)));
        
        boolean hasNewContent = false;
        TemporalTriple triple;
        while ((triple = reader.readNext()) != null) {
            if (triple.getTime() <= windowEnd) {
                content.add(triple);
            } else {
                break;
            }
            hasNewContent = true;
        }
        
        if(hasNewContent) {
            final TripleWindow w = new TripleWindow(
                    new ArrayList<>(content), 
                    windowStart, 
                    windowEnd);
            
            //TODO: why content.add(triple) here again?
            if(triple != null) {
                content.add(triple);
            }
            
            return w;
        } else {
            return null;
        }
    }
}