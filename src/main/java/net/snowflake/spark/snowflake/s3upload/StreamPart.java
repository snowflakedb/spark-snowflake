package net.snowflake.spark.snowflake.s3upload;

import java.io.InputStream;

/**
 * A simple class which holds some data which can be uploaded to S3 as part of a multipart upload and a part number
 * identifying it.
 */
class StreamPart {

    private ConvertibleOutputStream stream;
    private int partNumber;

    /**
     * A 'poison pill' placed on the queue to indicate that there are no further parts from a stream.
     */
    static final StreamPart POISON = new StreamPart(null, -1);

    public StreamPart(ConvertibleOutputStream stream, int partNumber) {
        this.stream = stream;
        this.partNumber = partNumber;
    }

    public int getPartNumber() {
        return partNumber;
    }

    public ConvertibleOutputStream getOutputStream() {
        return stream;
    }

    public InputStream getInputStream() {
        return stream.toInputStream();
    }

    public long size() {
        return stream.size();
    }

    @Override
    public String toString() {
        return String.format("[Part number %d %s]", partNumber,
                stream == null ?
                        "with null stream" :
                        String.format("containing %.2f MB", size() / (1024 * 1024.0)));
    }
}
