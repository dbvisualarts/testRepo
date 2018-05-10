package com.datamantra.utils.avro;

/**
 * Created by kafka on 10/10/17.
 */
public class Constants {
    public static final String KAFKA_SCHEMA_REGISTRY_URL = "http://schemRegisteryIP:port/schema-repo";//TODO make configurable

    public static final Integer ZOOKEEPER_SESSION_TIMEOUT = 30000;
    public static final Integer ZOOKEEPER_CONNECTION_TIMEOUT = 30000;
}
