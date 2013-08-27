package com.snda.games.storm.mam.schema;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Author: bj
 * Time: 2013-08-21 3:25 PM
 * Desc: Schema for FilterUriBolt.
 */
public class AlogScheme implements Scheme {
    @Override
    public List<Object> deserialize(byte[] ser) {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("url_id", "time_local");
    }
}
