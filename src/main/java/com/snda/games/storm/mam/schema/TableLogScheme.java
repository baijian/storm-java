package com.snda.games.storm.mam.schema;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;

import java.util.List;

public class TableLogScheme implements Scheme {

    @Override
    public List<Object> deserialize(byte[] bytes) {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("tableName", "logJson");
    }
}
