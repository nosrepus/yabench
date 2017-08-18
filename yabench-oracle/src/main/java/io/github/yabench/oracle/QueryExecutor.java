package io.github.yabench.oracle;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.expr.aggregate.AggregateRegistry;
import com.hp.hpl.jena.util.FileManager;
import io.github.yabench.commons.utils.NodeUtils;
import io.github.yabench.oracle.sparql.AccAvg;
import org.apache.jena.atlas.logging.Log;

import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class QueryExecutor {

    private final Query query;
    private static Model m;

    public QueryExecutor(final String template, final Properties variables) {
        this.query = QueryFactory.create(resolveVars(template, variables));
        String static_file = "/home/l36gao/UWaterloo-WatDiv/bin/Release/1-10/1-10.ttl";
        m = ModelFactory.createDefaultModel();
        try{
            InputStream in = FileManager.get().open(static_file);
            if(in == null){
                throw new IllegalArgumentException("File" + " not found");
            }
            m.read(in, null, "TURTLE");
        } catch(Exception e){
            StringReader sr = new StringReader(static_file);
            try{
                m.read(sr, null, "RDF/XML");
            } catch(Exception e1){
                try{
                    sr = new StringReader(static_file);
                    m.read(sr, null, "TURTLE");
                } catch(Exception e2){
                    try{
                        sr = new StringReader(static_file);
                        m.read(sr, null, "N-TRIPLE");
                    } catch(Exception e3){
                        sr = new StringReader(static_file);
                        m.read(sr, null, "RDF/JSON");
                    }
                }
            }
            sr.close();
        }
    }

    static {
        registerCustomAggregates();
    }

    public BindingWindow executeSelect(final TripleWindow input) {
        Model data = ModelFactory.createDefaultModel();
        data.add(input.getModel());
        data.add(m);
        try (QueryExecution qexec = QueryExecutionFactory.create(query, data)) {
            ResultSet results = qexec.execSelect();
            final List<Binding> bindings = new ArrayList<>();
            while (results.hasNext()) {
                final QuerySolution soln = results.next();
                bindings.add(NodeUtils.toBinding(soln));
            }
            return new BindingWindow(bindings, input.getStart(), input.getEnd());
        }
    }

    private String resolveVars(final String template, final Properties vars) {
        String result = new String(template);
        for (String key : vars.stringPropertyNames()) {
            result = result.replaceAll("\\$\\{" + key + "\\}", vars.getProperty(key));
        }
        return result;
    }

    private static void registerCustomAggregates() {
        AggregateRegistry.register("http://yabench/avg", new AccAvg.Factory());
    }

}
