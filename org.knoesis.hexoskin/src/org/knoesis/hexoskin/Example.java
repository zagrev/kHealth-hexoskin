/**
 *
 */
package org.knoesis.hexoskin;

import info.aduna.iteration.Iterations;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.openrdf.model.Model;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.Rio;
import org.openrdf.sail.nativerdf.NativeStore;

/**
 *
 */
public class Example
{
   /** the logger */
   private static Logger log;

   /**
    * an example program using Sesame RDF4j
    *
    * @param argv
    * @throws RepositoryException
    * @throws RDFHandlerException
    * @throws IOException
    * @throws RDFParseException
    */
   public static void main(final String[] argv) throws RepositoryException, RDFHandlerException, RDFParseException,
         IOException
   {
      final URL resource = Example.class.getResource("/log4j.properties");
      // PropertyConfigurator.configure(resource);
      BasicConfigurator.configure();
      log = Logger.getLogger(Example.class);

      // final Repository repo = new SailRepository(new ForwardChainingRDFSInferencer(new NativeStore(new File(
      // "C:/Users/sbetts/Documents/Minethurn/WSU/7900 Web 3.0/Hexoskin"))));
      final Repository repo = new SailRepository(new NativeStore(new File(
            "C:/Users/sbetts/Documents/Minethurn/WSU/7900 Web 3.0/Hexoskin")));
      repo.initialize();

      final String namespace = "http://knoesis.org/hexoskin/1.0.2#";
      final ValueFactory factory = repo.getValueFactory();
      final URI john = factory.createURI(namespace, "HeartRateObservation");

      final RepositoryConnection conn = repo.getConnection();
      try
      {
         final File ontoFile = new File(
               "C:/Users/sbetts/Documents/Minethurn/WSU/7900 Web 3.0/Hexoskin/hexoskin1.0.2.owl");
         conn.add(ontoFile, namespace, RDFFormat.RDFXML);

         final Model model = Iterations.addAll(conn.getStatements(null, null, null, true), new LinkedHashModel());
         model.setNamespace("rdf", RDF.NAMESPACE);
         model.setNamespace("rdfs", RDFS.NAMESPACE);
         model.setNamespace("xsd", XMLSchema.NAMESPACE);
         model.setNamespace("foaf", FOAF.NAMESPACE);
         model.setNamespace("hexoskin", namespace);
         model.setNamespace("DUL", "http://www.loa-cnr.it/ontologies/DUL.owl#");
         model.setNamespace("owl", "http://www.w3.org/2002/07/owl#");
         model.setNamespace("ssn", "http://purl.oclc.org/NET/ssnx/ssn#");
         model.setNamespace("FMA", "http://purl.org/obo/owl/FMA#");

         Rio.write(model, System.out, RDFFormat.TURTLE);
         System.out.println("");
      }
      finally
      {
         conn.close();
      }
      log.debug("Done");
   }
}
