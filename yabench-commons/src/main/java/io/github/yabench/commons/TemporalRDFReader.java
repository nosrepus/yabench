package io.github.yabench.commons;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TemporalRDFReader implements Closeable, AutoCloseable {

    // s,p,o + timestamp/interval = 4
    private static final int TUPLE_SIZE = 4;
    private static final String SPACE = " ";
    private final BufferedReader reader;
	private static final Logger logger = LoggerFactory.getLogger(TemporalRDFReader.class);
    private TemporalTriple lastTriple = null;

    public TemporalRDFReader(File stream) throws IOException {
        this(stream.toPath());
    }

    public TemporalRDFReader(Path stream) throws IOException {
        this(Files.newBufferedReader(stream));
    }

    public TemporalRDFReader(Reader reader) {
        this.reader = new BufferedReader(reader);
    }

    /**
     * @return null if the end of the stream has been reached
     * @throws IOException
     */
    public TemporalTriple readNextTriple() throws IOException {
        final String line = reader.readLine();
        if (line != null) {
	    
	    String[] tuple = line.split("\t");
            Resource subject = ResourceFactory.createResource(tuple[0]);
            Property predicate = ResourceFactory.createProperty(tuple[1]);
            RDFNode object = createObject(tuple[2]);

            Statement stmt = ResourceFactory.createStatement(
                    subject, predicate, object);

            long time = Long.parseLong(tuple[3]);

            return new TemporalTriple(stmt, time);
            
	    /*
	    String[] tuple = line.split(SPACE, TUPLE_SIZE + 1);
            Resource subject = ResourceFactory.createResource(
                    tuple[0].substring(1, tuple[0].length() - 1));
            Property predicate = ResourceFactory.createProperty(
                    tuple[1].substring(1, tuple[1].length() - 1));
            RDFNode object = createObject(tuple[2]);

            Statement stmt = ResourceFactory.createStatement(
                    subject, predicate, object);
            long time = Long.parseLong(tuple[TUPLE_SIZE - 1]
                    .substring(1, tuple[TUPLE_SIZE - 1].length() - 1));

            return new TemporalTriple(stmt, time);
	    */
        } else {
            return null;
        }
    }

    public TemporalGraph readNextGraph() throws IOException {
        final List<TemporalTriple> triples = new ArrayList<>();
        //following lines commented out, because last triple is added already in #85 now
        //if (lastTriple != null) {
        //    triples.add(lastTriple);
        //}
	for(;;) {
	    if(lastTriple==null){
		lastTriple = readNextTriple();	    
		if(lastTriple == null) break;
	    }
	    triples.add(lastTriple);
	    final TemporalTriple triple = readNextTriple();
	    if(triple==null||triple.getTime()!=lastTriple.getTime()){
		lastTriple = triple;
		break;
	    }
	    lastTriple = triple;
	}
/*
        for (;;) {
            final TemporalTriple triple = readNextTriple();
            if (triple != null) {
                if (lastTriple == null) {
                    triples.add(triple);
                    lastTriple = triple;
                } else {
                    if (lastTriple.getTime() == triple.getTime()) {
                        triples.add(triple);
                        lastTriple = triple;
                    } else {
                    	triples.add(triple);
                        lastTriple = triple;
                        break;
                    }
                }
            } else {
                lastTriple = null;
                break;
            }
        }
*/
        return triples.isEmpty() ? null : new TemporalGraph(triples);
    }

    /**
     * @param string
     * @return
     */
    private RDFNode createObject(String tuple) {
        //if it is datatyped...
        if (tuple.contains("\"")) {
            String objectString = tuple.substring(1, tuple.length()-1);
            if(isNum(objectString)){
	    	return ResourceFactory.createTypedLiteral(objectString, XSDDatatype.XSDint);
	    }else
		return ResourceFactory.createTypedLiteral(objectString);
        } else {
            return ResourceFactory.createResource(
                    tuple);
        }
/*
        if (tuple.contains("^^")) {
            String[] objectSplit = tuple.split("\\^\\^");
            String objectString = objectSplit[0];
            String dtype = objectSplit[1];

            //if it is a float datatype...
            if (dtype.toLowerCase().contains("float")) {
                return ResourceFactory.createTypedLiteral(
                        objectString, XSDDatatype.XSDfloat);
            } else {
                return ResourceFactory.createTypedLiteral(objectString);
            }
        } else {
            return ResourceFactory.createResource(
                    tuple.substring(1, tuple.length() - 1));
        }
*/
    }

    private Boolean isNum(String in){
        for(int i = 0; i<in.length(); i++){
            if(in.charAt(i)>'9'||in.charAt(i)<'0') return false;
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

}
