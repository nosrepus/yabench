package io.github.yabench.engines.cqels;

import com.hp.hpl.jena.rdf.model.Statement;
import io.github.yabench.engines.commons.AbstractEngine;
import io.github.yabench.engines.commons.Query;
import io.github.yabench.engines.commons.ResultListener;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import org.deri.cqels.engine.ContinuousListener;
import org.deri.cqels.engine.ContinuousSelect;
import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.RDFStream;
import java.io.*;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CQELSEngine extends AbstractEngine {

    private static final String STREAM_URI = "http://ex.org/streams/test";
    private static final String CQELS_HOME = "cqels_home";
    private ExecContext execContext;
    private ContinuousListener resultListener;
    private RDFStream rdfStream;
    private static final Logger logger = LoggerFactory.getLogger(CQELSEngine.class);
    private Writer writer = null;
    private long base_time = 0;
    private Boolean init_time = false;


    public CQELSEngine() {
        File home = new File(CQELS_HOME);
        if (!home.exists()) {
            home.mkdir();
        }
        this.execContext = new ExecContext(CQELS_HOME, true);
//	this.execContext.loadDefaultDataset("/home/l36gao/UWaterloo-WatDiv/bin/Release/1-10/1-10.ttl");
        this.execContext.loadDataset("http://dsg.uwaterloo.ca/watdiv/knowledge", "/hdd1/watdiv/data/1-100.ttl");
        try{
            this.writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("input_timestamp.txt"), "utf-8"));
        }catch(Exception ex){

        }

    }

    @Override
    public void initialize() {
        rdfStream = new RDFStream(execContext, STREAM_URI) {

            @Override
            public void stop() {
                //Nothing
            }
        };
    }

    @Override
    public void registerQuery(final Query query, final ResultListener listener)
            throws ParseException {
        resultListener = new CQELSResultListenerProxy(execContext, listener);

        if (resultListener != null) {
            ContinuousSelect select = execContext
                    .registerSelect(query.getQueryString());
            select.register(resultListener);
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public void stream(Statement stmt) {
        //logger.info(stmt.toString());
        //logger.info("time:"+System.currentTimeMillis());
        long time = System.currentTimeMillis();
        if(!init_time){
            init_time = true;
            base_time = time;
        }
	rdfStream.stream(stmt.asTriple());
        
	try {
            writer.write(stmt.getSubject().toString()+'\t');
            writer.write(stmt.getPredicate().toString()+'\t');
            writer.write(stmt.getObject().toString()+'\t');
            writer.write(Long.toString(time-base_time)+'\n');
            writer.flush();
	}catch(Exception ex){

        }
	
    }

    @Override
    public void close() throws IOException {
        resultListener = null;
        rdfStream.stop();
        execContext = null;
    }

}
