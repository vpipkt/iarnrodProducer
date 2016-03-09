/**
 * Created by jbrown on 1/4/16.
 */


//package com.ccri.geomesa.iarnrod;

import org.apache.commons.cli.*;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureStore;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.joda.time.DateTime;
import org.locationtech.geomesa.kafka.KafkaDataStoreHelper;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.locationtech.geomesa.utils.text.WKTUtils$;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

import java.io.IOException;
import java.util.*;

/***********************************************************************
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 *************************************************************************/

public class junkProducer {
    public static final String KAFKA_BROKER_PARAM = "brokers";
    public static final String ZOOKEEPERS_PARAM = "zookeepers";
    public static final String ZK_PATH = "zkPath";
    // META DATA CONSTANTS
    public static final String API_PATH = "http://api.irishrail.ie/realtime/realtime.asmx/getCurrentTrainsXML";
    public static final String TRAIN_STATUS = "TrainStatus";
    public static final String TRAIN_LAT = "TrainLatitude";
    public static final String TRAIN_LON = "TrainLongitude";
    public static final String TRAIN_CODE = "TrainCode";
    public static final String PUBLIC_MESSAGE = "PublicMessage";
    public static final String DIRECTION = "Direction";
    public static final String TRAIN_DATE = "TrainDate";

    public static final String[] KAFKA_CONNECTION_PARAMS = new String[] {
            KAFKA_BROKER_PARAM,
            ZOOKEEPERS_PARAM,
            ZK_PATH
    };

    // reads and parse the command line args
    public static Options getCommonRequiredOptions() {
        Options options = new Options();

        Option kafkaBrokers = OptionBuilder.withArgName(KAFKA_BROKER_PARAM)
                .hasArg()
                .isRequired()
                .withDescription("The comma-separated list of Kafka brokers, e.g. localhost:9092")
                .create(KAFKA_BROKER_PARAM);
        options.addOption(kafkaBrokers);

        Option zookeepers = OptionBuilder.withArgName(ZOOKEEPERS_PARAM)
                .hasArg()
                .isRequired()
                .withDescription("The comma-separated list of Zookeeper nodes that support your Kafka instance, e.g.: zoo1:2181,zoo2:2181,zoo3:2181")
                .create(ZOOKEEPERS_PARAM);
        options.addOption(zookeepers);

        Option zkPath = OptionBuilder.withArgName(ZK_PATH)
                .hasArg()
                .withDescription("Zookeeper's discoverable path for metadata, defaults to /geomesa/ds/kafka")
                .create(ZK_PATH);
        options.addOption(zkPath);

        return options;
    }

    // construct connection parameters for the DataStoreFinder
    public static Map<String, String> getKafkaDataStoreConf(CommandLine cmd) {
        Map<String, String> dsConf = new HashMap<String, String>();
        for (String param : KAFKA_CONNECTION_PARAMS) {
            dsConf.put(param, cmd.getOptionValue(param));
        }
        return dsConf;
    }

    // add a SimpleFeature to the producer every half second
    public static void addSimpleFeatures(SimpleFeatureType sft, FeatureStore producerFS)
            throws InterruptedException, IOException {

        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);

        while (true) {
            // write the SimpleFeatures to Kafka
            producerFS.addFeatures(simjunk(builder));

            // wait some seconds  in between updating SimpleFeatures to create a stream of data
            Thread.sleep(30 * 1000);
        }
    }

    // prints out attribute values for a SimpleFeature
    public static void printFeature(SimpleFeature f) {
        Iterator<Property> props = f.getProperties().iterator();
        int propCount = f.getAttributeCount();
        System.out.print("fid:" + f.getID());
        for (int i = 0; i < propCount; i++) {
            Name propName = props.next().getName();
            System.out.print(" | " + propName + ":" + f.getAttribute(propName));
        }
        System.out.println();
    }

    public static DefaultFeatureCollection simjunk(final SimpleFeatureBuilder builder){
        final DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();

        Random rng;
        rng = new Random();
        String trainCode;

        // add three features with static meta junk, and random point
        builder.add("Good");
        trainCode = "Train A";
        builder.add(trainCode);
        builder.add("");
        builder.add("unknown");
        builder.add(DateTime.now().toDate());
        double lon = -180 + 360 * rng.nextDouble();
        double lat  = -90 + 180 * rng.nextDouble();
        builder.add(WKTUtils$.MODULE$.read("POINT(" + (lon) + " " + (lat) + ")"));
        SimpleFeature feature = builder.buildFeature(trainCode);
        featureCollection.add(feature);

        // add three features with static meta junk, and random point
        builder.reset();
        builder.add("Good");
        trainCode = "Train B";
        builder.add(trainCode);
        builder.add("");
        builder.add("unknown");
        builder.add(DateTime.now().toDate());
        lon = -180 + 360 * rng.nextDouble();
        lat  = -90 + 180 * rng.nextDouble();
        builder.add(WKTUtils$.MODULE$.read("POINT(" + (lon) + " " + (lat) + ")"));
        SimpleFeature feature2 = builder.buildFeature(trainCode);
        featureCollection.add(feature2);

        // add three features with static meta junk, and random point
        builder.reset();
        builder.add("Good");
        trainCode = "Train C";
        builder.add(trainCode);
        builder.add("");
        builder.add("unknown");
        builder.add(DateTime.now().toDate());
        lon = -180 + 360 * rng.nextDouble();
        lat  = -90 + 180 * rng.nextDouble();
        builder.add(WKTUtils$.MODULE$.read("POINT(" + (lon) + " " + (lat) + ")"));
        SimpleFeature feature3 = builder.buildFeature(trainCode);
        featureCollection.add(feature3);


        return featureCollection;
    }


    public static void main(String[] args) throws Exception {
        // read command line args for a connection to Kafka
        CommandLineParser parser = new BasicParser();
        Options options = getCommonRequiredOptions();
        CommandLine cmd = parser.parse(options, args);

        // create the producer and consumer KafkaDataStore objects
        Map<String, String> dsConf = getKafkaDataStoreConf(cmd);
//        dsConf.put("isProducer", "true");
        DataStore producerDS = DataStoreFinder.getDataStore(dsConf);

        // verify that we got back our KafkaDataStore objects properly
        if (producerDS == null) {
            throw new Exception("Null producer KafkaDataStore");
        }

        // create the schema which creates a topic in Kafka
        // (only needs to be done once)
        final String sftName = "junk";
        final String sftSchema = "trainStatus:String,trainCode:String,publicMessage:String,direction:String,dtg:Date,*geom:Point:srid=4326";
        SimpleFeatureType sft = SimpleFeatureTypes.createType(sftName, sftSchema);
        // set zkPath to default if not specified
        String zkPath = (dsConf.get(ZK_PATH) == null) ? "/geomesa/ds/kafka" : dsConf.get(ZK_PATH);
                SimpleFeatureType preppedOutputSft = KafkaDataStoreHelper.createStreamingSFT(sft, zkPath);
        // only create the schema if it hasn't been created already
        if (!Arrays.asList(producerDS.getTypeNames()).contains(sftName))
            producerDS.createSchema(preppedOutputSft);


        // the live consumer must be created before the producer writes features
        // in order to read streaming data.
        // i.e. the live consumer will only read data written after its instantiation
        SimpleFeatureStore producerFS = (SimpleFeatureStore) producerDS.getFeatureSource(sftName);

        // creates and adds SimpleFeatures to the producer on an interval
        System.out.println("Writing features to Kafka... refresh GeoServer layer preview to see changes");
        addSimpleFeatures(sft, producerFS);

        System.exit(0);
    }
}

