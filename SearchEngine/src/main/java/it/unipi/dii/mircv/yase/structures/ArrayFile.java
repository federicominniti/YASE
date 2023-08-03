package it.unipi.dii.mircv.yase.structures;

import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Skeleton for a class that implements an array of Entry objects ordered by Key.
 * Entries all have the same length in bytes.
 * @param <Key> the object responsible for ordering inside each Entry
 * @param <Entry> the actual objects stored in the file
 */
public abstract class ArrayFile<Key extends Comparable<Key>, Entry extends IdentifiableObject<Key>> {
    // path of the folder where this file is stored on disk
    protected final String path;
    // name of the file on disk
    protected final String fileName;
    // max length for any string in the file
    protected final int maxStringLength;
    // fixed size of an entry
    protected final int ENTRY_SIZE;
    // cache to speed-up I/O when possible
    protected LRUMap<Key, Entry> cache;
    protected RandomAccessFile filePointer;
    private final static Logger logger = Logger.getLogger(ArrayFile.class);

    /**
     * Constructor of a disk based array file with fixed size entries
     * @param path
     * @param fileName
     * @param maxStringLength
     */
    protected ArrayFile(final String path, final String fileName, final int maxStringLength) {
        this.path = path;
        this.fileName = fileName;
        this.maxStringLength = maxStringLength;
        this.ENTRY_SIZE = getEntrySize(maxStringLength);
    }

    /**
     * Not all ArrayFiles have cache enabled. The DocumentIndex is an array file without
     * cache enabled.
     * @param cacheSize the max number of entries to keep in cache
     */
    protected void enableCache(int cacheSize) {
        this.cache = new LRUMap<>(cacheSize);
    }

    /**
     * Method used by the constructor to initialize the ENTRY_SIZE parameter.
     * It depends on the type of Entry we are going to store in this file.
     * @param maxStringLength the max length of any string in the file
     */
    protected abstract int getEntrySize(final int maxStringLength);

    /**
     * Reads a single entry from the file using a RandomAccessFile object already
     * initialized and moved to the correct offset.
     */
    protected abstract Entry readEntry() throws IOException;

    /**
     * Writes a single entry to a MappedByteBuffer object already initialized and
     * moved to the correct offset.
     * @param entry
     * @param buffer
     */
    protected abstract void writeEntry(final Entry entry, final MappedByteBuffer buffer);

    /**
     * Abstract method that defines the logic to retrieve an Entry from the file.
     * @param key the key of the entry to retrieve
     */
    protected abstract Entry retrieveEntry(final Key key) throws IOException;


    /**
     * Retrieves an Entry from the ArrayFile using the logic defined in the @{retrieveEntry} function.
     * If cache is enabled, checks whether the Entry is already present in cache. If not, it retrieves
     * the Entry from the file on disk, adding it to cache.
     * @param key the key to lookup in the file
     */
    public Entry get(final Key key) {
        if (!isFileOpen())
            openFile(true);

        // check if the entry is saved in cache
        if (cache != null && cache.get(key) != null) {
            return cache.get(key);
        } else {
            // not present in cache, so search on the disk
            Entry entry = null;
            try {
                entry = retrieveEntry(key);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }

            // add the new entry in cache
            if (cache != null && entry != null) {
                cache.put(key, entry);
            }

            return entry;
        }
    }

    /**
     * Moves a RandomAccessFile object to the specified offset and reads an Entry
     * @param offset
     */
    protected Entry getAtPosition(final long offset) {
        try {
            filePointer.seek(offset);
            return readEntry();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        return null;
    }

    /**
     * Gets the i-th Entry object stored in this file.
     * @param index
     */
    public Entry getAtIndex(final int index) {
        return getAtPosition((long)index*ENTRY_SIZE);
    }

    /**
     * Finds an entry in the file depending on the Key, using binary search.
     * (NB: This was used by the Lexicon before it became an hash table)
     * @param key
     */
    protected Entry binarySearch(final Key key) {
        long end;
        try {
            end = filePointer.length()/ENTRY_SIZE - 1;
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return null;
        }

        long start = 0;
        long mid = (start + end)/2;
        while (start <= end) {
            Entry entryAtMid = getAtPosition(mid*ENTRY_SIZE);
            Key keyAtMid = entryAtMid.getID();
            int compare = key.compareTo(keyAtMid);
            if (compare > 0) {
                start = mid + 1;
            } else if (compare == 0) {
                return entryAtMid;
            } else {
                end = mid - 1;
            }

            mid = (start + end)/2;
        }

        return null;
    }

    /**
     * Puts a List of entries to the file. Ordering is assumed correct and sorting
     * (if needed) should be done before calling this method.
     * @param entries
     */
    public void putAllMMap(final List<Entry> entries) {
        try {
            // open the file with the pointer starting from the beginning
            openFile(true);

            // prepare a buffer with a size enough big for all the entries
            int bufferSize = entries.size() * ENTRY_SIZE;
            MappedByteBuffer buffer = filePointer.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, bufferSize);

            // write each entry in the buffer
            for (Entry entry: entries) {
                writeEntry(entry, buffer);
            }

            //force changes on disk before closing
            buffer.force();

        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * Opens the file of this disk based array file
     * @param atZero is true if the file is opened at the beginning, false if at the end
     */
    public void openFile(boolean atZero) {
        if (filePointer != null)
            return;
        try {
            Path directoriesPath = Paths.get(path);
            Files.createDirectories(directoriesPath);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        try {
            filePointer = new RandomAccessFile(path+fileName, "rw");
            if (atZero)
                filePointer.seek(0);
            else
                filePointer.seek(filePointer.length());
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * Closes the array file.
     */
    public void closeFile() {
        try {
            if (filePointer != null && filePointer.getChannel().isOpen()) {
                filePointer.close();
                filePointer = null;
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * Checks if the array file is open
     */
    public boolean isFileOpen() {
        return (filePointer != null && filePointer.getChannel().isOpen());
    }

    /**
     * Checks the number of entries in this array file, by looking at the file's length
     */
    public int getNumEntries() {
        try {
            if (!isFileOpen())
                openFile(true);
            return Long.valueOf(filePointer.length() / ENTRY_SIZE).intValue();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return -1;
    }

    /**
     * Cleans the directory linked to this array file. Useful during indexing.
     */
    public void deleteDirectory(){
        try {
            FileUtils.cleanDirectory(new File(path));
            FileUtils.deleteDirectory(new File(path));
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    };
}

