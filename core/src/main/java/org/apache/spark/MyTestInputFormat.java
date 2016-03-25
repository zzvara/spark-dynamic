package org.apache.spark;

import org.apache.hadoop.mapred.TextInputFormat;

public class MyTestInputFormat extends TextInputFormat {

    @Override
    protected long computeSplitSize(long goalSize, long minSize, long blockSize) {
        return goalSize + 1;
    }
}
