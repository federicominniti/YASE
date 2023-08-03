package it.unipi.dii.mircv.yase.structures;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.log4j.Logger;

import java.io.*;
import java.math.BigInteger;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * Extends an ArrayFile to implement a simplified HashTable on disk with open addressing and double
 * hashing strategy to counter collisions. The implementation is simplified with respect to a standard
 * hash table because we do not need to delete entries.
 * @param <Key> the type identifying an Entry
 * @param <Entry> the type of entries objects
 */
public abstract class HashTableFile<Key extends Comparable<Key>, Entry extends IdentifiableObject<Key>> extends ArrayFile<Key, Entry> {
    //the size of the open addressed HashTable
    protected long SIZE;
    //hash functions, murmur3 with different seeds
    private final HashFunction firstMurmur = Hashing.murmur3_128(0);
    private final HashFunction secondMurmur = Hashing.murmur3_128(1);
    private final static Logger logger = Logger.getLogger(HashTableFile.class);

    /**
     * Constructor for a disk-based hash table
     * @param path
     * @param fileName
     * @param maxStringLength
     * @param maxNumObjects
     * @param cacheSize
     */
    public HashTableFile(String path, String fileName, int maxStringLength, int maxNumObjects, int cacheSize) {
        //call constructor for ArrayFile
        super(path, fileName, maxStringLength);

        //enable cache to avoid searching in the hashtable if possible
        enableCache(cacheSize);

        //returns the size of the HashTable given the max number of entries
        SIZE = getTableSize(maxNumObjects);
    }

    /**
     * Retrieves an entry stored in the file through double hashing.
     * @param key
     */
    protected Entry retrieveEntry(Key key) {
        // compute hashes
        int firstHash = firstHash(key);
        int secondHash = secondHash(key);
        //when there is no collision the cycle breaks immediately
        Key tmpKey;
        Entry res;
        int i = 0;
        int pos = firstHash;

        do {
            res = getAtIndex(pos);
            // if the cell is empty, the entry is not present
            if (res == null)
                return null;
            // check if you found the correct key
            tmpKey = res.getID();
            if (tmpKey.compareTo(key) == 0) {
                return res;
            }
            else {
                i++;
                //to avoid overflowing into negative numbers, force the MSB to be 0 before
                //computing the mod
                pos = (int) (((firstHash + i*secondHash) >>> 1) % SIZE);
            }
        } while (true);
    }

    /**
     * During merge a List of entries will be constructed. This function
     * is called to store the list of entries into a disk-based hashtable.
     * @param entries
     */
    public void putAllMMap(List<Entry> entries) {
        //init file with all zeros
        try {
            FileOutputStream fos = new FileOutputStream(path+fileName);
            BufferedOutputStream bos = new BufferedOutputStream(fos);
            bos.write(new byte[(int)SIZE*ENTRY_SIZE]);
            bos.flush();
            fos.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }


        try {

            openFile(true);
            MappedByteBuffer buffer = filePointer.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, SIZE*ENTRY_SIZE);

            //use a bitset instead of an array of booleans to avoid wasting too much memory to check
            //collisions
            BitSet checkCollision = new BitSet((int) SIZE);
            for (Entry entry: entries) {
                int firstHash = firstHash(entry.getID());
                int secondHash = secondHash(entry.getID());
                int index = firstHash;
                int i = 0;
                while (checkCollision.get(index)) {
                    i++;
                    //calc double hash, force MSB to 0
                    //calc modulo and cast to int
                    index = (int) (((firstHash + i*secondHash) >>> 1) % SIZE);
                }

                //set bit in bitset to remember the cell already contains an entry
                checkCollision.set(index);
                buffer.position(index*ENTRY_SIZE);
                writeEntry(entry, buffer);
            }
            //force changes on disk before closing
            buffer.force();
            closeFile();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

    }

    /**
     * Returns the size of the hashtable. It should be at least 1.3 times greater than the maximum
     * number of entries.
     * @param maxNumObjects the max number of entries that will be stored in the table
     */
    private int getTableSize(int maxNumObjects) {
        BigInteger integer = BigInteger.valueOf((int)(1.8 * maxNumObjects));
        return integer.nextProbablePrime().intValue();
    }

    /**
     * Uses the first murmur on a Key to get a pseudo-random index of the table.
     * @param k
     */
    private int firstHash(Key k) {
        //convert the Key into a String and calculate the first murmur on its bytes,
        //setting the MSB to zero to avoid having negative hashes
        //then calc modulo on the size of the table and convert to int
        long hash = firstMurmur.hashBytes(k.toString().getBytes()).asLong() >>> 1;
        return (int) (hash % SIZE);
    }

    /**
     * Uses the second murmur on a Key to get a pseudo-random hash.
     * @param k
     */
    private int secondHash(Key k) {
        //convert the Key into a String and calculate the second murmur on its bytes,
        //setting the MSB to zero to avoid having negative hashes.
        //then calc modulo on the size of the table and convert to int, adding 1 to avoid
        //having the second hash equal to 0 (there would be a collision on the cell pointed
        //by the first hash)
        long hash = secondMurmur.hashBytes(k.toString().getBytes()).asLong() >>> 1;
        return (int) ((hash % SIZE) + 1);
    }
}
