package net.snowflake.spark.snowflake.parquet;

import org.apache.parquet.io.SeekableInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Convert an {@link InputStream} object to {@link SeekableInputStream}
 */
public class SeekableInputStreamBuilder extends SeekableInputStream{

    // large data may crash this system. try to use file less than 1G
    private ArrayList<Byte> data;

    private int pos;

    private long mark = 0;

    private int count;

    public SeekableInputStreamBuilder(InputStream input) throws IOException {
        this(input,input.available());
    }

    /**
     * Create a new {@link SeekableInputStream} by caching all input data
     * @param input an {@link InputStream} contains all data
     * @throws IOException is an I/O error occurs
     */
    public SeekableInputStreamBuilder(InputStream input, int size) throws IOException {
        // input.available may not be the real size of input stream,
        // only use this value to initialize the ArrayList.
        // Do not use data large than 1G due to the size limitation of List.
        data = new ArrayList<>(size);
        int c;
        while((c=input.read())!=-1){
            data.add((byte)c);
        }
        input.close();
        this.pos = 0;
        this.count = data.size();
    }

    @Override
    public long getPos() throws IOException {
        return pos;
    }

    @Override
    public void seek(long newPos) throws IOException {
        if(newPos<0 || newPos>=count) throw new IOException("incorrect seek position: "+newPos);
        pos = (int) newPos;
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
        readFully(bytes, 0, bytes.length);
    }

    @Override
    public void readFully(byte[] bytes, int start, int len) throws IOException {
        read(bytes,start,len);
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
        int size = count-pos;
        while(pos<count) buf.put(data.get(pos++));
        return size;
    }

    @Override
    public void readFully(ByteBuffer buf) throws IOException {
        read(buf);
    }

    @Override
    public int read() throws IOException {
        if(pos==count) throw new EOFException("size: "+count+" pos: "+pos);
        return (data.get(pos++).intValue()+256)%256;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException{
        if(off+len>b.length) throw new IOException("Array out of bound. size of array: "
                + b.length+" start: "+off+" len: "+len);
        if(pos+len>count) throw new EOFException("Stream size: "+count+" pos: "+pos
                + " len: "+len);
        for (int i = 0; i < len; i++) {
            b[off+i]=data.get(pos++);
        }
        return len;
    }

    @Override
    public void close() throws IOException {
        // nothing to close
    }

    public int getLength(){
        return count;
    }

}