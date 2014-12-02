/**
 *
 */
package org.knoesis.hexoskin;

import info.aduna.iteration.Iterations;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FilenameUtils;
import org.openrdf.model.Literal;
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
import org.openrdf.rio.Rio;
import org.openrdf.sail.nativerdf.NativeStore;

import au.com.bytecode.opencsv.CSVReader;

/**
 *
 */
public class ReadDataIntoStore
{
   /**
    * @param args
    * @throws IOException
    * @throws FileNotFoundException
    * @throws RepositoryException
    * @throws RDFHandlerException
    */
   public static void main(final String[] args) throws FileNotFoundException, IOException, RepositoryException,
         RDFHandlerException
   {
      final ReadDataIntoStore reader = new ReadDataIntoStore();

      String dataDir = "C:/Users/sbetts/Documents/Minethurn/WSU/7900 Web 3.0/Hexoskin/data";
      String repoDir = "C:/Users/sbetts/Documents/Minethurn/WSU/7900 Web 3.0/Hexoskin/repo";
      String outputFilename = "C:/Users/sbetts/Documents/Minethurn/WSU/7900 Web 3.0/Hexoskin/hexoskin.turtle";

      final Options options = reader.createOptions();

      try
      {
         final BasicParser parser = new BasicParser();
         final CommandLine cmdLine = parser.parse(options, args);

         if (cmdLine.hasOption("dataDir"))
         {
            dataDir = cmdLine.getOptionValue("dataDir");
         }

         if (cmdLine.hasOption("repoDir"))
         {
            repoDir = cmdLine.getOptionValue("repoDir");
         }

         if (cmdLine.hasOption("outputFilename"))
         {
            outputFilename = cmdLine.getOptionValue("outputFilename");
         }
      }
      catch (final ParseException e)
      {
         final HelpFormatter formatter = new HelpFormatter();
         formatter.printHelp("reader", options);
         return;
      }

      boolean error = false;
      if (!reader.validateDirectory(dataDir))
      {
         System.err.println("Invalid data directory: " + dataDir);
         error = true;
      }

      if (!reader.validateDirectory(repoDir))
      {
         System.err.println("Invalid repository directory: " + repoDir);
         error = true;
      }
      final File outputDir = new File(outputFilename).getParentFile();
      if (!outputDir.exists() || !outputDir.isDirectory())
      {
         System.err.println("Invalid output file location: " + outputDir);
         error = true;
      }

      if (!error)
      {
         reader.createRepo(repoDir);
         reader.processDir(dataDir);
         reader.writeModel(outputFilename);
      }
   }

   /** the data directory option */
   @SuppressWarnings("static-access")
   private static Option        optionDataDir     = OptionBuilder
                                                        .withArgName("dir")
                                                        .hasArg()
                                                        .withDescription(
                                                              "The directory that contains the data files to process")
                                                        .create("dataDir");

   /** the repository directory option */
   @SuppressWarnings("static-access")
   private static Option        optionRepoDir     = OptionBuilder
                                                        .withArgName("dir")
                                                        .hasArg()
                                                        .withDescription(
                                                              "The directory that will contain the repository")
                                                        .create("repoDir");

   /** the location to write the output */
   @SuppressWarnings("static-access")
   private static Option        optOutputFilename = OptionBuilder
                                                        .withArgName("dir")
                                                        .hasArg()
                                                        .withDescription(
                                                              "The filename to contain all the TURTLE output")
                                                        .create("outputFilename");

   /** The Feature of Interest class for Heart */
   private URI                  classFeatureOfInterestHeart;

   /**
    *
    */
   private URI                  classHeartRateProperty;

   /** the measurement capability class for heart rate */
   private URI                  classMeasurementCapabilityHeartRate;

   /**
    *
    */
   private URI                  classObservationHeartRate;

   /** the observation value for heart rate property */
   private URI                  classObservationValueHeartRate;
   /** the sensor for heart rate property */
   private URI                  classSensingDeviceHeartRate;
   /** the Sensing Heart Rate */
   private URI                  classSensingHeartRate;
   /** the sensor input for heart rate property */
   private URI                  classSensorInputHeartRate;
   /** the sensor output for heart rate property */
   private URI                  classSensorOutputHeartRate;
   /** the name of the current file */
   private String               currentName;
   /** the identifier of the current person */
   private String               currentPid;
   /** the date of the current file's data */
   private Date                 currentRecordDate;
   /** the sensor we are currently processing */
   private URI                  currentSensor;
   /** the type of the current data */
   private String               currentType;

   /** data property that contains the actual value */
   private URI                  dataPropHasDataValue;
   /** the data property for unit of measure */
   private URI                  dataPropUom;
   /** the hexoskin namespace */
   final String                 hexoskin          = "http://knoesis.org/hexoskin/1.0.2#";
   /**
    *
    */
   private URI                  instanceFeatureOfInterestHeart;
   /**
    *
    */
   private URI                  instanceHeartRateProperty;

   /**
    *
    */
   private URI                  instanceMeasurementCapabilityHeartRate;

   /**
    *
    */
   private URI                  instanceObservationHeartRate;

   /**
    *
    */
   private URI                  instanceSensingDeviceHeartRate;

   /**
    *
    */
   private URI                  instanceSensingHeartRate;

   /**
    *
    */
   private URI                  instanceSensorInputHeartBeart;

   /**
    *
    */
   private URI                  instanceSensorOutputHeartRate;

   /** the Beats Per Minute unit of measure */
   private URI                  instanceUomBpm;
   /** property for "detects" */
   private URI                  propertyDetectsHeartRate;

   /**
    *
    */
   private URI                  propertyFeatureOfInterestHeart;

   /** link between MeasurementCapability and Property */
   private URI                  propertyForPropertyHeartRate;

   /**
    *
    */
   private URI                  propertyHasMeasurementCapabilityHeartRate;

   /**
    *
    */
   private URI                  propertyHasPropertyHeartRate;

   /** property for "hasValue" */
   private URI                  propertyHasValueHeartRate;

   /** links sensor and sensing */
   private URI                  propertyImplementsHeartRate;

   /** property for "includesEvent" */
   private URI                  propertyIncludesEventHeartRate;

   /** property for "isProducedBy" */
   private URI                  propertyIsProducedByHeartRateSensor;

   /**
    *
    */
   private URI                  propertyIsProxyForHeartRate;

   /** The observation result for heart rate property */
   private URI                  propertyObservationResultHeartRate;

   /** the property for "observationResultTime" */
   private URI                  propertyObservationResultTime;

   /** the property for "observationSamplingTime" */
   private URI                  propertyObservationSamplingTime;

   /**
    *
    */
   private URI                  propertyObservedByHeartRate;
   /**
    *
    */
   private URI                  propertyObservedPropertyHeartRate;
   /**
    *
    */
   private URI                  propertyObservesHeartRate;
   /**
    *
    */
   private URI                  propertySensingMethodUsedHeartRate;
   /** the connection to the triple store */
   private RepositoryConnection repoConn;

   /** the factory to create new values */
   private ValueFactory         valueFactory;

   /**
    * create an observation in the dataset
    *
    * @param hexoseconds
    *           The time in hexoseconds, which is 1/256 of a second
    * @param value
    *           the data value
    * @throws RepositoryException
    */
   private void createHeartRateObservation(final String hexoseconds, final String value) throws RepositoryException
   {
      // convert the time into a real date
      final long hxSec = Long.parseLong(hexoseconds);
      final long seconds = hxSec / 256;
      final Calendar instance = Calendar.getInstance();
      final long remain = hxSec % 256;
      final double ms = remain * 1000 / 256.0;
      instance.setTimeInMillis(seconds * 1000 + (long) ms);
      final Date valueDate = instance.getTime();

      // get necessary properties
      // create all the model objects
      final URI observationValue = valueFactory.createURI(hexoskin, "Heart_Rate_Observation_Value_" + currentPid + "_"
            + hexoseconds);

      repoConn.add(observationValue, RDFS.SUBCLASSOF, classObservationValueHeartRate);
      repoConn.add(instanceObservationHeartRate, propertyHasValueHeartRate, observationValue);

      final Literal literal = valueFactory.createLiteral(Integer.parseInt(value)); // want this to be a numeric type
      repoConn.add(observationValue, dataPropHasDataValue, literal);

      repoConn.add(observationValue, dataPropUom, instanceUomBpm);
      repoConn.add(observationValue, propertyObservationResultTime, valueFactory.createLiteral(valueDate));
      repoConn.add(observationValue, propertyObservationSamplingTime, valueFactory.createLiteral(currentRecordDate));
   }

   /**
    * create a new person to link to all this data
    *
    * @throws RepositoryException
    */
   private void createNewPersonHeartRate() throws RepositoryException
   {
      instanceFeatureOfInterestHeart = valueFactory.createURI(hexoskin, "Person_Heart_" + currentPid);

      // create an instance of the heart rate sensor class
      instanceObservationHeartRate = valueFactory.createURI(hexoskin, "Hexoskin_Model_A_Heart_Rate_Observation_v1_"
            + currentPid);
      instanceSensingDeviceHeartRate = valueFactory.createURI(hexoskin,
            "Hexoskin_Model_A_Heart_Rate_Sensing_Device_v1_" + currentPid);
      instanceSensorInputHeartBeart = valueFactory.createURI(hexoskin, "Heart_Beat_" + currentPid);
      instanceHeartRateProperty = valueFactory.createURI(hexoskin, "Heart_Rate_Property_" + currentPid);
      instanceSensingHeartRate = valueFactory.createURI(hexoskin, "Heart_Rate_Sensing_" + currentPid);
      instanceSensorOutputHeartRate = valueFactory.createURI(hexoskin, "Hexoskin_Model_A_Heart_Rate_Output_v1_"
            + currentPid);
      instanceMeasurementCapabilityHeartRate = valueFactory.createURI(hexoskin,
            "Hexoskin_Model_A_Heart_Rate_Measurement_Capability_v1_" + currentPid);

      // currentPerson is a natural person, has a body, the body has a heart,
      // the heart if a feature of interest
      repoConn.add(instanceSensingDeviceHeartRate, RDFS.SUBCLASSOF, classSensingDeviceHeartRate);
      repoConn.add(instanceFeatureOfInterestHeart, RDFS.SUBCLASSOF, classFeatureOfInterestHeart);
      repoConn.add(instanceObservationHeartRate, RDFS.SUBCLASSOF, classObservationHeartRate);
      repoConn.add(instanceSensorInputHeartBeart, RDFS.SUBCLASSOF, classSensorInputHeartRate);
      repoConn.add(instanceHeartRateProperty, RDFS.SUBCLASSOF, classHeartRateProperty);
      repoConn.add(instanceSensingHeartRate, RDFS.SUBCLASSOF, classSensingHeartRate);
      repoConn.add(instanceSensorOutputHeartRate, RDFS.SUBCLASSOF, classSensorOutputHeartRate);
      repoConn.add(instanceMeasurementCapabilityHeartRate, RDFS.SUBCLASSOF, classMeasurementCapabilityHeartRate);

      repoConn.add(instanceFeatureOfInterestHeart, propertyHasPropertyHeartRate, instanceHeartRateProperty);

      repoConn.add(instanceObservationHeartRate, propertyObservationResultHeartRate, instanceSensorOutputHeartRate);
      repoConn.add(instanceObservationHeartRate, propertyIncludesEventHeartRate, instanceSensorInputHeartBeart);
      repoConn.add(instanceObservationHeartRate, propertyObservedByHeartRate, instanceSensingDeviceHeartRate);
      repoConn.add(instanceObservationHeartRate, propertySensingMethodUsedHeartRate, instanceSensingHeartRate);
      repoConn.add(instanceObservationHeartRate, propertyObservedPropertyHeartRate, instanceFeatureOfInterestHeart);
      repoConn.add(instanceObservationHeartRate, propertyFeatureOfInterestHeart, instanceFeatureOfInterestHeart);

      repoConn.add(instanceSensorInputHeartBeart, propertyIsProxyForHeartRate, instanceHeartRateProperty);

      repoConn.add(instanceSensorOutputHeartRate, propertyIsProducedByHeartRateSensor, instanceSensingDeviceHeartRate);

      repoConn.add(instanceSensingDeviceHeartRate, propertyDetectsHeartRate, instanceSensorInputHeartBeart);
      repoConn.add(instanceSensingDeviceHeartRate, propertyObservesHeartRate, instanceHeartRateProperty);
      repoConn.add(instanceSensingDeviceHeartRate, propertyHasMeasurementCapabilityHeartRate,
            instanceMeasurementCapabilityHeartRate);
      repoConn.add(instanceSensingDeviceHeartRate, propertyImplementsHeartRate, instanceSensingHeartRate);

      repoConn.add(instanceMeasurementCapabilityHeartRate, propertyForPropertyHeartRate, instanceHeartRateProperty);
   }

   /**
    * create the command line option information
    *
    * @return the configured options for the command line
    */
   private Options createOptions()
   {
      final Options options = new Options();

      options.addOption(optionDataDir);
      options.addOption(optionRepoDir);
      options.addOption(optOutputFilename);

      return options;
   }

   /**
    * @param repoDir
    *           the directory to contain the new repository
    * @throws RepositoryException
    */
   private void createRepo(final String repoDir) throws RepositoryException
   {
      final Repository repo = new SailRepository(new NativeStore(new File(repoDir), "spoc,posc,opsc"));
      // final Repository repo = new SailRepository(new MemoryStore());
      repo.initialize();

      repoConn = repo.getConnection();
      valueFactory = repoConn.getValueFactory();

      // create all the URIs for the parent classes
      classFeatureOfInterestHeart = valueFactory.createURI(hexoskin, "Heart");
      classSensorInputHeartRate = valueFactory.createURI(hexoskin, "Heart_Beat");

      // now create all the classes needed to create an observation for Heart
      // Rate
      classObservationValueHeartRate = valueFactory.createURI(hexoskin, "Heart_Rate_Observation_Value");
      classObservationHeartRate = valueFactory.createURI(hexoskin, "Heart_Rate_Observation_v1");
      classSensorOutputHeartRate = valueFactory.createURI(hexoskin, "Heart_Rate_Output");
      classSensingDeviceHeartRate = valueFactory.createURI(hexoskin, "Hexoskin_Model_A_Heart_Rate_Sensor_v1");
      classMeasurementCapabilityHeartRate = valueFactory.createURI(hexoskin,
            "Hexoskin_Model_A_Heart_Rate_Measurement_Capability_v1");
      classSensingHeartRate = valueFactory.createURI(hexoskin, "Hexoskin_Model_A_Heart_Rate_Sensing_v1");
      classHeartRateProperty = valueFactory.createURI(hexoskin, "Heart_Rate_Property");

      // create all the relations for the observation of Heart Rate
      propertyObservationResultHeartRate = valueFactory.createURI(hexoskin, "observationResultHeartRate");
      propertyForPropertyHeartRate = valueFactory.createURI(hexoskin, "forPropertyHeartRate");
      propertyHasValueHeartRate = valueFactory.createURI(hexoskin, "hasValueHeartRate");
      propertyIsProducedByHeartRateSensor = valueFactory.createURI(hexoskin, "isProducedByHeartRateSensor");
      propertyDetectsHeartRate = valueFactory.createURI(hexoskin, "detectsHeartRate");
      propertyIncludesEventHeartRate = valueFactory.createURI(hexoskin, "includesEventHeartRate");
      propertyIsProxyForHeartRate = valueFactory.createURI(hexoskin, "isProxyForHeartRate");
      propertyObservedPropertyHeartRate = valueFactory.createURI(hexoskin, "observed_Heart_Rate_property");
      propertyImplementsHeartRate = valueFactory.createURI(hexoskin, "implementsHeartRate");
      propertyObservesHeartRate = valueFactory.createURI(hexoskin, "observes_Heart_Rate");
      propertyHasPropertyHeartRate = valueFactory.createURI(hexoskin, "hasPropertyHeartRate");
      propertyObservedByHeartRate = valueFactory.createURI(hexoskin, "observedByHeartRateSensor");
      propertyFeatureOfInterestHeart = valueFactory.createURI(hexoskin, "featureOfInterestHeart");
      propertySensingMethodUsedHeartRate = valueFactory.createURI(hexoskin, "sensingMethodUsedHeartRate");
      propertyHasMeasurementCapabilityHeartRate = valueFactory.createURI(hexoskin, "hasMeasurementCapabilityHeartRate");

      propertyObservationResultTime = valueFactory.createURI("http://purl.oclc.org/NET/ssnx/ssn#observationResultTime");
      propertyObservationSamplingTime = valueFactory
            .createURI("http://purl.oclc.org/NET/ssnx/ssn#observationSamplingTime");
      dataPropHasDataValue = valueFactory
            .createURI("http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#hasDataValue");
      dataPropUom = valueFactory.createURI("http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#UnitOfMeasure");

      instanceUomBpm = valueFactory.createURI(hexoskin, "bpm");
   }

   /**
    * @param file
    *           the filename to process
    */
   private void parseFilename(final File file)
   {
      currentName = file.getName();
      final String[] filenameParts = FilenameUtils.getBaseName(currentName).split("-");
      if (filenameParts.length != 8)
      {
         System.err.println("Filename does not match expected format - skipping " + currentName);
         throw new IllegalArgumentException("Invalid filename format: " + file.getName());
      }

      currentPid = filenameParts[0];
      currentType = filenameParts[1];

      final Calendar cal = Calendar.getInstance();
      cal.set(Integer.parseInt(filenameParts[2]), Integer.parseInt(filenameParts[3]),
            Integer.parseInt(filenameParts[4]), Integer.parseInt(filenameParts[5]), Integer.parseInt(filenameParts[6]),
            Integer.parseInt(filenameParts[7]));
      cal.set(Calendar.MILLISECOND, 0);

      currentRecordDate = cal.getTime();
   }

   /**
    * read all the CSV files in the directory and create Observations for them
    *
    * @param filename
    * @throws IOException
    * @throws FileNotFoundException
    * @throws RepositoryException
    * @throws RDFHandlerException
    */
   private void processDir(final String filename) throws FileNotFoundException, IOException, RepositoryException,
         RDFHandlerException
   {
      final File dataDir = new File(filename);
      System.out.println("Using data directory: " + dataDir);

      // get the data files
      final File[] dataFiles = dataDir.listFiles(new FilenameFilter()
      {
         @Override
         public boolean accept(final File dir, final String name)
         {
            return name.contains("heartrate") && (name.endsWith("csv") || name.endsWith("CSV"));
         }
      });
      // want to make sure we only create each person and sensor once
      Arrays.sort(dataFiles);

      // FIXME testing with just 1 file to start
      final File[] testFiles = dataFiles;
      // testFiles[0] = dataFiles[0];

      int count = 0;

      for (final File file : testFiles)
      {
         final String oldPid = currentPid;

         System.out.println("processing " + file);

         try
         {
            parseFilename(file);

            // did we change people?
            if (oldPid != currentPid)
            {
               createNewPersonHeartRate();
            }

            repoConn.begin();
            count += processFileHeartRate(file);
            repoConn.commit();

            System.out.println(String.format("  %,d records", Integer.valueOf(count)));
         }
         catch (final NumberFormatException e)
         {
            System.err.println("Cannot read the date from the file format");
         }
      }
   }

   /**
    * @param file
    * @throws IOException
    * @throws FileNotFoundException
    * @throws RepositoryException
    */
   private int processFileHeartRate(final File file) throws FileNotFoundException, IOException, RepositoryException
   {
      String[] nextLine;
      int count = 0;

      try (FileReader fileReader = new FileReader(file); CSVReader reader = new CSVReader(fileReader))
      {
         while ((nextLine = reader.readNext()) != null)
         {
            createHeartRateObservation(nextLine[0], nextLine[1]);
            count++;
         }
      }
      return count;
   }

   /**
    * verify that the directory exists and is readable
    *
    * @param dataDir
    *           the directory to validate
    * @return <code>true</code> of the directory exists, <code>false</code> otherwise
    */
   private boolean validateDirectory(final String dataDir)
   {
      final File dir = new File(dataDir);

      return dir.exists() && dir.isDirectory();
   }

   /**
    * @param filename
    * @throws RepositoryException
    * @throws RDFHandlerException
    * @throws IOException
    */
   private void writeModel(String filename) throws RepositoryException, RDFHandlerException, IOException
   {
      System.out.println("writing " + filename);

      final Model model = Iterations.addAll(repoConn.getStatements(null, null, null, true), new LinkedHashModel());

      model.setNamespace("rdf", RDF.NAMESPACE);
      model.setNamespace("rdfs", RDFS.NAMESPACE);
      model.setNamespace("xsd", XMLSchema.NAMESPACE);
      model.setNamespace("foaf", FOAF.NAMESPACE);
      model.setNamespace("hexoskin", hexoskin);
      // model.setNamespace("DUL", "http://www.loa-cnr.it/ontologies/DUL.owl#");
      model.setNamespace("DUL", "http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#");
      model.setNamespace("owl", "http://www.w3.org/2002/07/owl#");
      model.setNamespace("ssn", "http://purl.oclc.org/NET/ssnx/ssn#");
      model.setNamespace("FMA", "http://purl.org/obo/owl/FMA#");

      final FileOutputStream out = new FileOutputStream(filename);
      Rio.write(model, out, RDFFormat.TURTLE);
      out.write("\n".getBytes());
   }
}
