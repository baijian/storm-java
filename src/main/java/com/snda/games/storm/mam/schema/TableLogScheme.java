package com.snda.games.storm.mam.schema;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;

import java.util.List;

/**
 * Author: bj
 * Time: 9/4/13 12:02 PM
 * Desc: Different log map to tables scheme.
 */
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
