package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by deepak on 4/28/17.
 */

public class Value {
    String value;
    long version;
    Value(String value1, long version1)
    {
        value = value1;
        version = version1;
    }
}