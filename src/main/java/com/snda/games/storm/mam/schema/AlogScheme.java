package com.snda.games.storm.mam.schema;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;

import java.util.List;

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
