package it.unipi.dii.mircv.yase.indexing;

import it.unipi.dii.mircv.yase.structures.ArrayFile;
import it.unipi.dii.mircv.yase.structures.DocumentDescriptor;
import it.unipi.dii.mircv.yase.util.Configuration;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

/**
 * Class to handle the DocumentIndex file which stores information of the documents of the collection.
 * Each document is described by a {@link DocumentDescriptor} class, which has a docId, a fixed-length docNo
 * and a docLength. This implementation extends the {@link ArrayFile} class. It is used for both final and partial
 * DocumentIndex.
 */
public class DocumentIndex extends ArrayFile<Integer, DocumentDescriptor> {
    private final static Logger logger = Logger.getLogger(DocumentIndex.class);
    private long currentOffset;
    private DataInputStream fileReader;
    private DataOutputStream fileWriter;
    private DocumentDescriptor lastRead;
    private MappedByteBuffer memoryMappedFile;

    /**
     * {@link DocumentIndex} constructor.
     * @param path path to the DocumentIndex directory
     * @param fileName name of the file
     * @param maxStringLength max length of the docNo
     */
    public DocumentIndex(final String path, final String fileName, final int maxStringLength) {
        // invoke the constructor of ArrayFile
        super(path, fileName, maxStringLength);
        lastRead = null;
        memoryMappedFile = null;
    }

    /**
     * Method to prepare the {@link DocumentIndex} to response to query requests.
     *
     * @throws IOException if there are errors while preparing
     */
    public void prepareToAnswerQuery() throws IOException {
        // set the pointer to the beginning of the file
        currentOffset = 0;
        filePointer.seek(currentOffset);
        lastRead = null;

        // buffered reader used to retrieve documents descriptor quickly
        fileReader = new DataInputStream(new BufferedInputStream(new FileInputStream(filePointer.getFD()),
                1024*Configuration.documentIndexLocalBufferSize));
    }

    /**
     * Prepares the document index to write DocumentDescriptor entries in sequence during indexing
     * @throws IOException
     */
    public void prepareForBatchWrites() throws IOException {
        openFile(true);
        fileWriter = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filePointer.getFD())));
    }

    /**
     * Closes the DataOutputStream used to write the document indexing during the first indexing phase
     * @throws IOException
     */
    public void closeWriter() throws IOException {
        fileWriter.close();
        filePointer = null;
    }

    /**
     * Method used to speed enable the {@link MappedByteBuffer} in order to speed up the merging phase.
     */
    public void enableMemoryMap() {
        try {
            memoryMappedFile = filePointer.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, filePointer.length());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Method used to retrieve the document by its docid
     * @param docid of the document that I am searching
     * @return document information
     */
    public DocumentDescriptor getAtIndexMMap(int docid) {
        // align the beginning of the buffer with the beginning of doc's information
        memoryMappedFile.position(docid*ENTRY_SIZE);
        return readEntry(memoryMappedFile);
    }


    /**
     * Reads a single DocumentDescriptor using a RandomAccessFile object
     * already positioned at the correct offset.
     *
     * @return document descriptor
     */
    @Override
    protected DocumentDescriptor readEntry() throws IOException {
        // docid computed by position
        int docId = (int)filePointer.getFilePointer()/ENTRY_SIZE;
        // read document length
        int docLen = filePointer.readShort();
        // read the docNo
        byte[] buf = new byte[maxStringLength];
        filePointer.read(buf);
        String docNo = new String(buf, StandardCharsets.US_ASCII);

        return new DocumentDescriptor(docId, docNo, docLen);
    }

    /**
     * Reads a single DocumentDescriptor from a {@link MappedByteBuffer} buffer
     * already positioned at the correct offset.
     *
     * @param buffer
     * @return document descriptor
     */
    protected DocumentDescriptor readEntry(MappedByteBuffer buffer) {
        // docid computed by position
        int docId = buffer.position() / ENTRY_SIZE;
        // read document length
        int docLen = buffer.getShort();
        // read the docNo
        byte[] buf = new byte[maxStringLength];
        buffer.get(buf);
        String docNo = new String(buf, StandardCharsets.US_ASCII);

        return new DocumentDescriptor(docId, docNo, docLen);
    }

    /**
     * Reads a single DocumentDescriptor from a {@link DataInputStream} reader
     * already positioned at the correct offset. Used in when we have to retrieve
     * a document information for a query.
     * @param reader
     * @return document descriptor
     */
    protected DocumentDescriptor readEntry(DataInputStream reader) throws IOException {
        // docid computed by position
        int docId = (int) (currentOffset/ENTRY_SIZE);
        // read document length
        int docLen = reader.readShort();
        // read the docNo
        byte[] buf = new byte[maxStringLength];
        reader.read(buf);
        String docNo = new String(buf, StandardCharsets.US_ASCII);
        // update the offset skipping the entry just read
        currentOffset += ENTRY_SIZE;
        return new DocumentDescriptor(docId, docNo, docLen);
    }

    /**
     * Writes a single DocumentDescriptor to an already opened MappedByteBuffer.
     * @param entry information to write on file
     * @param buffer where you want to write it
     */
    @Override
    protected void writeEntry(final DocumentDescriptor entry, final MappedByteBuffer buffer) {
        // prepare the docNo and add spaces if they are necessary
        String fixedSizeDocNo = String.format("%1$"+maxStringLength+ "s", entry.getDocNo());
        // write the document length
        buffer.putShort((short)entry.getDocLen());
        // write the docNo
        buffer.put(fixedSizeDocNo.getBytes(StandardCharsets.US_ASCII));
    }

    /**
     * append the {@link DocumentDescriptor} entry to the {@link DocumentIndex}.
     * @param entry document descriptor
     */
    protected void append(final DocumentDescriptor entry) throws IOException {
        // prepare the docNo and add spaces if they are necessary
        String fixedSizeDocNo = String.format("%1$"+maxStringLength+ "s", entry.getDocNo());
        // write the document length
        fileWriter.writeShort((short)entry.getDocLen());
        // write the docNo
        fileWriter.write(fixedSizeDocNo.getBytes(StandardCharsets.US_ASCII));
    }

    /**
     * Method to retrieve {@link DocumentDescriptor} of a
     * @param docid during query processing
     * @return document descriptor
     */
    public DocumentDescriptor getAtGEQIndex(final int docid) {
        // check if the last doc read is the searched one
        if (lastRead != null && lastRead.getDocId() == docid)
            return lastRead;

        // jump to the position of the document descriptor, computed with the docid
        DocumentDescriptor result = getAtGEQPosition((long)docid*ENTRY_SIZE);
        if (result != null) {
            lastRead = result;
        }
        return result;
    }

    /**
     * Method used to move forward the DataInputStream in order to retrieve the {@link DocumentDescriptor}
     * during query processing
     *
     * @param offset
     * @return
     */
    private DocumentDescriptor getAtGEQPosition(final long offset) {
        try {
            // move the pointer forward until it reach the beginning of the searched doc descriptor
            fileReader.skipBytes((int)(offset-currentOffset));
            // update the current offset of the pointer
            currentOffset = offset;
            return readEntry(fileReader);

        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        return null;
    }

    /**
     * Returns the fixed size of a single DocumentDescriptor.
     */
    @Override
    protected int getEntrySize(final int maxStringLength) {
        // docNo length + document length
        return maxStringLength + Short.BYTES;
    }

    @Override
    protected DocumentDescriptor retrieveEntry(final Integer docid) {
        return getAtIndex(docid);
    }
}
