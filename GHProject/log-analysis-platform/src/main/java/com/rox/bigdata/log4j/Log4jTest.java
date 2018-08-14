package com.rox.bigdata.log4j;

import org.apache.log4j.Logger;
import org.junit.Test;

public class Log4jTest {
    Logger logger = Logger.getLogger(Log4jTest.class);

    @Test
    public void testLog4j() {
        logger.debug("---debug---");
        logger.info("--info--");
        logger.warn("----warn----");
        logger.error("-----error---");
    }


    @Test
    public void testUserLog4j() {
        logger = Logger.getLogger("access");
        logger.debug("---debug---");
        logger.info("--info--");
        logger.warn("----warn----");
        logger.error("-----error---");
    }


}
