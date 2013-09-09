package com.snda.games.storm.mam.schema;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class StringScheme implements Scheme {
    @Override
    public List<Object> deserialize(byte[] ser) {
        try {
            return new Values(new String(ser, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("str");
    }
}
