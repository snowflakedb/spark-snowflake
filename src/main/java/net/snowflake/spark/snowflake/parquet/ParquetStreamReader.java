/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package net.snowflake.spark.snowflake.parquet;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.*;
import org.apache.parquet.format.*;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter.MetadataFilter;
import org.apache.parquet.hadoop.metadata.*;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.counters.BenchmarkCounter;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.parquet.bytes.BytesUtils.readIntLittleEndian;
import static org.apache.parquet.hadoop.ParquetFileWriter.MAGIC;

/**
 * overwrite ParquetFileReader to support read parquet file from {@link InputStream}
 *
 * As an example @see <a href="https://github.com/binglihub/parquet-io/blob/master/src/main/scala/parquet/io/ExampleReader.scala">Example parquet reader</a>
 */
public class ParquetStreamReader {
    /**
     * Reads and decompresses a dictionary page for the given column chunk.
     *
     * Returns null if the given column chunk has no dictionary page.
     *
     * @param meta a column's ColumnChunkMetaData to read the dictionary from
     * @return an uncompressed DictionaryPage or null
     * @throws IOException
     */
    DictionaryPage readDictionary(ColumnChunkMetaData meta) throws IOException {
        if (!meta.getEncodings().contains(Encoding.PLAIN_DICTIONARY) &&
                !meta.getEncodings().contains(Encoding.RLE_DICTIONARY)) {
            return null;
        }

        // TODO: this should use getDictionaryPageOffset() but it isn't reliable.
        if (f.getPos() != meta.getStartingPos()) {
            f.seek(meta.getStartingPos());
        }

        PageHeader pageHeader = Util.readPageHeader(f);
        if (!pageHeader.isSetDictionary_page_header()) {
            return null; // TODO: should this complain?
        }

        DictionaryPage compressedPage = readCompressedDictionary(pageHeader, f);
        CodecFactory.BytesDecompressor decompressor = codecFactory.getDecompressor(meta.getCodec());

        return new DictionaryPage(
                decompressor.decompress(compressedPage.getBytes(), compressedPage.getUncompressedSize()),
                compressedPage.getDictionarySize(),
                compressedPage.getEncoding());
    }
    private DictionaryPage readCompressedDictionary(
            PageHeader pageHeader, SeekableInputStream fin) throws IOException {
        DictionaryPageHeader dictHeader = pageHeader.getDictionary_page_header();

        int uncompressedPageSize = pageHeader.getUncompressed_page_size();
        int compressedPageSize = pageHeader.getCompressed_page_size();

        byte [] dictPageBytes = new byte[compressedPageSize];
        fin.readFully(dictPageBytes);

        BytesInput bin = BytesInput.from(dictPageBytes);

        return new DictionaryPage(
                bin, uncompressedPageSize, dictHeader.getNum_values(),
                converter.getEncoding(dictHeader.getEncoding()));
    }

    /**
     * Reads the meta data in the footer of the file.
     * Skipping row groups (or not) based on the provided filter
     * @param input the Parquet File stream
     * @return the metadata with row groups filtered.
     * @throws IOException  if an error occurs while reading the file
     */
    public static ParquetMetadata readFooter(InputFile input) throws IOException {
        return readFooter(input, ParquetMetadataConverter.NO_FILTER);
    }

    /**
     * Reads the meta data block in the footer of the file using provided input stream
     * @param file a {@link InputFile} to read
     * @param filter the filter to apply to row groups
     * @return the metadata blocks in the footer
     * @throws IOException if an error occurs while reading the file
     */
    public static final ParquetMetadata readFooter(
            InputFile file, MetadataFilter filter) throws IOException {
        ParquetMetadataConverter converter = new ParquetMetadataConverter();
        SeekableInputStream in = file.newStream();
        try {
            return readFooter(converter, file.getLength(), file.toString(), in, filter);
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }

    /**
     * Reads the meta data block in the footer of the file using provided input stream
     * @param fileLen length of the file
     * @param filePath file location
     * @param f input stream for the file
     * @param filter the filter to apply to row groups
     * @return the metadata blocks in the footer
     * @throws IOException if an error occurs while reading the file
     */
    private static final ParquetMetadata readFooter(ParquetMetadataConverter converter, long fileLen, String filePath, SeekableInputStream f, MetadataFilter filter) throws IOException {
        LOG.debug("File length {}", fileLen);
        int FOOTER_LENGTH_SIZE = 4;
        if (fileLen < MAGIC.length + FOOTER_LENGTH_SIZE + MAGIC.length) { // MAGIC + data + footer + footerIndex + MAGIC
            throw new RuntimeException(filePath + " is not a Parquet file (too small)");
        }
        long footerLengthIndex = fileLen - FOOTER_LENGTH_SIZE - MAGIC.length;
        LOG.debug("reading footer index at {}", footerLengthIndex);

        f.seek(footerLengthIndex);
        int footerLength = readIntLittleEndian(f);
        byte[] magic = new byte[MAGIC.length];
        f.readFully(magic);
        if (!Arrays.equals(MAGIC, magic)) {
            throw new RuntimeException(filePath + " is not a Parquet file. expected magic number at tail " + Arrays.toString(MAGIC) + " but found " + Arrays.toString(magic));
        }
        long footerIndex = footerLengthIndex - footerLength;
        LOG.debug("read footer length: {}, footer index: {}", footerLength, footerIndex);
        if (footerIndex < MAGIC.length || footerIndex >= footerLengthIndex) {
            throw new RuntimeException("corrupted file: the footer index is not within the file");
        }
        f.seek(footerIndex);
        return converter.readParquetMetadata(f, filter);
    }


    /**
     * @param input the Parquet File stream
     * @param footer a {@link ParquetMetadata} footer already read from the file
     * @throws IOException if the file can not be opened
     */
    public ParquetStreamReader(InputFile input, ParquetMetadata footer) throws IOException {
        this.converter = new ParquetMetadataConverter();
        this.f = input.newStream();
        this.footer = footer;
        this.fileMetaData = footer.getFileMetaData();
        this.blocks = footer.getBlocks();
        for (ColumnDescriptor col : footer.getFileMetaData().getSchema().getColumns()) {
            paths.put(ColumnPath.get(col.getPath()), col);
        }
        this.codecFactory = new CodecFactory(new Configuration());
    }

    /**
     * Reads all the columns requested from the row group at the current file position.
     * @throws IOException if an error occurs while reading
     * @return the PageReadStore which can provide PageReaders for each column.
     */
    public PageReadStore readNextRowGroup() throws IOException {
        if (currentBlock == blocks.size()) {
            return null;
        }
        BlockMetaData block = blocks.get(currentBlock);
        if (block.getRowCount() == 0) {
            throw new RuntimeException("Illegal row group of 0 rows");
        }
        this.currentRowGroup = new ColumnChunkPageReadStore(block.getRowCount());
        // prepare the list of consecutive chunks to read them in one scan
        List<ParquetStreamReader.ConsecutiveChunkList> allChunks = new ArrayList<>();
        ParquetStreamReader.ConsecutiveChunkList currentChunks = null;
        for (ColumnChunkMetaData mc : block.getColumns()) {
            ColumnPath pathKey = mc.getPath();
            BenchmarkCounter.incrementTotalBytes(mc.getTotalSize());
            ColumnDescriptor columnDescriptor = paths.get(pathKey);
            if (columnDescriptor != null) {
                long startingPos = mc.getStartingPos();
                // first chunk or not consecutive => new list
                if (currentChunks == null || currentChunks.endPos() != startingPos) {
                    currentChunks = new ParquetStreamReader.ConsecutiveChunkList(startingPos);
                    allChunks.add(currentChunks);
                }
                currentChunks.addChunk(new ParquetStreamReader.ChunkDescriptor(columnDescriptor, mc, startingPos, (int)mc.getTotalSize()));
            }
        }
        // actually read all the chunks
        for (ParquetStreamReader.ConsecutiveChunkList consecutiveChunks : allChunks) {
            final List<ParquetStreamReader.Chunk> chunks = consecutiveChunks.readAll(f);
            for (ParquetStreamReader.Chunk chunk : chunks) {
                currentRowGroup.addColumn(chunk.descriptor.col, chunk.readAllPages());
            }
        }

        // avoid re-reading bytes the dictionary reader is used after this call
        if (nextDictionaryReader != null) {
            nextDictionaryReader.setRowGroup(currentRowGroup);
        }

        advanceToNextBlock();

        return currentRowGroup;
    }

    private boolean advanceToNextBlock() {
        if (currentBlock == blocks.size()) {
            return false;
        }

        // update the current block and instantiate a dictionary reader for it
        ++currentBlock;
        this.nextDictionaryReader = null;

        return true;
    }

    public FileMetaData getFileMetaData() {
        if (fileMetaData != null) {
            return fileMetaData;
        }
        return getFooter().getFileMetaData();
    }

    public ParquetMetadata getFooter() {
        return footer;
    }

    public void close() throws IOException {
        try {
            if (f != null) {
                f.close();
            }
        } finally {
            if (codecFactory != null) {
                codecFactory.release();
            }
        }
    }




    private Map<ColumnPath, ColumnDescriptor> paths = new HashMap<>();

    private SeekableInputStream f;

    private FileMetaData fileMetaData; // may be null

    private ParquetMetadata footer;

    private List<BlockMetaData> blocks;

    private static final Logger LOG = LoggerFactory.getLogger(ParquetStreamReader.class);

    private int currentBlock = 0;

    private ColumnChunkPageReadStore currentRowGroup = null;

    private ParquetMetadataConverter converter;

    private CodecFactory codecFactory;

    private DictionaryPageReader nextDictionaryReader = null;

    /**
     * The data for a column chunk
     *
     * @author Julien Le Dem
     *
     */
    private class Chunk extends ByteArrayInputStream {

        private final ParquetStreamReader.ChunkDescriptor descriptor;

        /**
         *
         * @param descriptor descriptor for the chunk
         * @param data contains the chunk data at offset
         * @param offset where the chunk starts in offset
         */
        public Chunk(ParquetStreamReader.ChunkDescriptor descriptor, byte[] data, int offset) {
            super(data);
            this.descriptor = descriptor;
            this.pos = offset;
        }

        protected PageHeader readPageHeader() throws IOException {
            return Util.readPageHeader(this);
        }

        /**
         * Read all of the pages in a given column chunk.
         * @return the list of pages
         */
        public ColumnChunkPageReadStore.ColumnChunkPageReader readAllPages() throws IOException {
            List<DataPage> pagesInChunk = new ArrayList<DataPage>();
            DictionaryPage dictionaryPage = null;
            PrimitiveType type = getFileMetaData().getSchema()
                    .getType(descriptor.col.getPath()).asPrimitiveType();
            long valuesCountReadSoFar = 0;
            while (valuesCountReadSoFar < descriptor.metadata.getValueCount()) {
                PageHeader pageHeader = readPageHeader();
                int uncompressedPageSize = pageHeader.getUncompressed_page_size();
                int compressedPageSize = pageHeader.getCompressed_page_size();
                switch (pageHeader.type) {
                    case DICTIONARY_PAGE:
                        // there is only one dictionary page per column chunk
                        if (dictionaryPage != null) {
                            throw new ParquetDecodingException("more than one dictionary page in column " + descriptor.col);
                        }
                        DictionaryPageHeader dicHeader = pageHeader.getDictionary_page_header();
                        dictionaryPage =
                                new DictionaryPage(
                                        this.readAsBytesInput(compressedPageSize),
                                        uncompressedPageSize,
                                        dicHeader.getNum_values(),
                                        converter.getEncoding(dicHeader.getEncoding())
                                );
                        break;
                    case DATA_PAGE:
                        DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
                        pagesInChunk.add(
                                new DataPageV1(
                                        this.readAsBytesInput(compressedPageSize),
                                        dataHeaderV1.getNum_values(),
                                        uncompressedPageSize,
                                        converter.fromParquetStatistics(
                                                getFileMetaData().getCreatedBy(),
                                                dataHeaderV1.getStatistics(),
                                                type),
                                        converter.getEncoding(dataHeaderV1.getRepetition_level_encoding()),
                                        converter.getEncoding(dataHeaderV1.getDefinition_level_encoding()),
                                        converter.getEncoding(dataHeaderV1.getEncoding())
                                ));
                        valuesCountReadSoFar += dataHeaderV1.getNum_values();
                        break;
                    case DATA_PAGE_V2:
                        DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
                        int dataSize = compressedPageSize - dataHeaderV2.getRepetition_levels_byte_length() - dataHeaderV2.getDefinition_levels_byte_length();
                        pagesInChunk.add(
                                new DataPageV2(
                                        dataHeaderV2.getNum_rows(),
                                        dataHeaderV2.getNum_nulls(),
                                        dataHeaderV2.getNum_values(),
                                        this.readAsBytesInput(dataHeaderV2.getRepetition_levels_byte_length()),
                                        this.readAsBytesInput(dataHeaderV2.getDefinition_levels_byte_length()),
                                        converter.getEncoding(dataHeaderV2.getEncoding()),
                                        this.readAsBytesInput(dataSize),
                                        uncompressedPageSize,
                                        converter.fromParquetStatistics(
                                                getFileMetaData().getCreatedBy(),
                                                dataHeaderV2.getStatistics(),
                                                type),
                                        dataHeaderV2.isIs_compressed()
                                ));
                        valuesCountReadSoFar += dataHeaderV2.getNum_values();
                        break;
                    default:
                        LOG.debug("skipping page of type {} of size {}", pageHeader.getType(), compressedPageSize);
                        this.skip(compressedPageSize);
                        break;
                }
            }
            if (valuesCountReadSoFar != descriptor.metadata.getValueCount()) {
                // Would be nice to have a CorruptParquetFileException or something as a subclass?
                throw new IOException(
                        "Expected " + descriptor.metadata.getValueCount() + " values in column chunk at input stream offset "
                                + descriptor.metadata.getFirstDataPageOffset() +
                                " but got " + valuesCountReadSoFar + " values instead over " + pagesInChunk.size()
                                + " pages ending at file offset " + (descriptor.fileOffset + pos()));
            }
            CodecFactory.BytesDecompressor decompressor = codecFactory.getDecompressor(descriptor.metadata.getCodec());
            return new ColumnChunkPageReadStore.ColumnChunkPageReader(decompressor, pagesInChunk, dictionaryPage);
        }

        /**
         * @return the current position in the chunk
         */
        public int pos() {
            return this.pos;
        }

        /**
         * @param size the size of the page
         * @return the page
         * @throws IOException
         */
        public BytesInput readAsBytesInput(int size) throws IOException {
            final BytesInput r = BytesInput.from(this.buf, this.pos, size);
            this.pos += size;
            return r;
        }

    }

    /**
     * deals with a now fixed bug where compressedLength was missing a few bytes.
     *
     * @author Julien Le Dem
     *
     */
    private class WorkaroundChunk extends ParquetStreamReader.Chunk {

        private final SeekableInputStream f;

        /**
         * @param descriptor the descriptor of the chunk
         * @param data contains the data of the chunk at offset
         * @param offset where the chunk starts in data
         * @param f the file stream positioned at the end of this chunk
         */
        private WorkaroundChunk(ParquetStreamReader.ChunkDescriptor descriptor, byte[] data, int offset, SeekableInputStream f) {
            super(descriptor, data, offset);
            this.f = f;
        }

        protected PageHeader readPageHeader() throws IOException {
            PageHeader pageHeader;
            int initialPos = this.pos;
            try {
                pageHeader = Util.readPageHeader(this);
            } catch (IOException e) {
                // this is to workaround a bug where the compressedLength
                // of the chunk is missing the size of the header of the dictionary
                // to allow reading older files (using dictionary) we need this.
                // usually 13 to 19 bytes are missing
                // if the last page is smaller than this, the page header itself is truncated in the buffer.
                this.pos = initialPos; // resetting the buffer to the position before we got the error
                LOG.info("completing the column chunk to read the page header");
                pageHeader = Util.readPageHeader(new SequenceInputStream(this, f)); // trying again from the buffer + remainder of the stream.
            }
            return pageHeader;
        }

        public BytesInput readAsBytesInput(int size) throws IOException {
            if (pos + size > count) {
                // this is to workaround a bug where the compressedLength
                // of the chunk is missing the size of the header of the dictionary
                // to allow reading older files (using dictionary) we need this.
                // usually 13 to 19 bytes are missing
                int l1 = count - pos;
                int l2 = size - l1;
                LOG.info("completed the column chunk with {} bytes", l2);
                return BytesInput.concat(super.readAsBytesInput(l1), BytesInput.copy(BytesInput.from(f, l2)));
            }
            return super.readAsBytesInput(size);
        }

    }


    /**
     * information needed to read a column chunk
     */
    private static class ChunkDescriptor {

        private final ColumnDescriptor col;
        private final ColumnChunkMetaData metadata;
        private final long fileOffset;
        private final int size;

        /**
         * @param col column this chunk is part of
         * @param metadata metadata for the column
         * @param fileOffset offset in the file where this chunk starts
         * @param size size of the chunk
         */
        private ChunkDescriptor(
                ColumnDescriptor col,
                ColumnChunkMetaData metadata,
                long fileOffset,
                int size) {
            super();
            this.col = col;
            this.metadata = metadata;
            this.fileOffset = fileOffset;
            this.size = size;
        }
    }

    /**
     * describes a list of consecutive column chunks to be read at once.
     *
     * @author Julien Le Dem
     */
    private class ConsecutiveChunkList {

        private final long offset;
        private int length;
        private final List<ParquetStreamReader.ChunkDescriptor> chunks = new ArrayList<>();

        /**
         * @param offset where the first chunk starts
         */
        ConsecutiveChunkList(long offset) {
            this.offset = offset;
        }

        /**
         * adds a chunk to the list.
         * It must be consecutive to the previous chunk
         * @param descriptor
         */
        public void addChunk(ParquetStreamReader.ChunkDescriptor descriptor) {
            chunks.add(descriptor);
            length += descriptor.size;
        }

        /**
         * @param f file to read the chunks from
         * @return the chunks
         * @throws IOException
         */
        public List<ParquetStreamReader.Chunk> readAll(SeekableInputStream f) throws IOException {
            List<ParquetStreamReader.Chunk> result = new ArrayList<>(chunks.size());
            f.seek(offset);
            byte[] chunksBytes = new byte[length];
            f.readFully(chunksBytes);
            // report in a counter the data we just scanned
            BenchmarkCounter.incrementBytesRead(length);
            int currentChunkOffset = 0;
            for (int i = 0; i < chunks.size(); i++) {
                ParquetStreamReader.ChunkDescriptor descriptor = chunks.get(i);
                if (i < chunks.size() - 1) {
                    result.add(new ParquetStreamReader.Chunk(descriptor, chunksBytes, currentChunkOffset));
                } else {
                    // because of a bug, the last chunk might be larger than descriptor.size
                    result.add(new ParquetStreamReader.WorkaroundChunk(descriptor, chunksBytes, currentChunkOffset, f));
                }
                currentChunkOffset += descriptor.size;
            }
            return result ;
        }

        /**
         * @return the position following the last byte of these chunks
         */
        public long endPos() {
            return offset + length;
        }

    }

}