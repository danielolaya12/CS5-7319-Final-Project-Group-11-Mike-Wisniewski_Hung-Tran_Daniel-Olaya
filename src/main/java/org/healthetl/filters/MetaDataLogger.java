package org.healthetl.filters;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class MetaDataLogger {
    private static final Logger logger = LogManager.getLogger(MetaDataLogger.class);

    //Capturing timestamp
    public static void logMetaData(String metaData) {
        logger.info(metaData);
    }

    //Testing MetaDataLogger class
    public static void main (String[] args) {
        logMetaData("Testing Logging Timestamp");
    }
}