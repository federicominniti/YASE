package it.unipi.dii.mircv.yase.structures;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * A simple class containing statistics about the index
 */
public class CollectionStatistics {
    // the size of our document collection
    private int numberOfDocuments;
    // the size of our document collection in terms of tokens (after preprocessing)
    private int collectionDocumentLength;
    // the size of our lexicon
    private int lexiconSize;
    private final Properties prop;
    private final String path = "SearchEngine/src/main/resources/statistics.properties";
    private static CollectionStatistics collectionStatistics = null;

    /**
     * Singleton pattern, returns the only instance of CollectionStatistics
     */
    public static CollectionStatistics getInstance(){
        if(collectionStatistics == null) {
            collectionStatistics = new CollectionStatistics();
        }
        return collectionStatistics;
    }

    /**
     * The private constructor reads from the "statistics.properties" file and loads
     * its content in memory
     */
    private CollectionStatistics(){
        prop = new Properties();
        try {
            prop.load(Files.newInputStream(Paths.get(path)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        numberOfDocuments = Integer.parseInt(prop.getProperty("numberOfDocuments"));
        collectionDocumentLength = Integer.parseInt(prop.getProperty("collectionDocumentLength"));
        lexiconSize = Integer.parseInt(prop.getProperty("lexiconSize"));
    }

    /**
     * Returns the average document length in the collection
     */
    public double getAverageDocumentLength(){
        return (double) collectionDocumentLength/numberOfDocuments;
    }

    /**
     * Returns the total number of documents in the collection
     */
    public int getNumberOfDocuments() {
        return numberOfDocuments;
    }

    /**
     * Sets the total number of documents in the collection
     * @param numberOfDocuments
     */
    public void setNumberOfDocuments(int numberOfDocuments) {
        this.numberOfDocuments = numberOfDocuments;
        prop.setProperty("numberOfDocuments", String.valueOf(numberOfDocuments));
        updateProperties();
    }

    /**
     * Sets the length of the collection in terms of tokens
     * @param collectionDocumentLength
     */
    public void setCollectionDocumentLength(int collectionDocumentLength) {
        this.collectionDocumentLength = collectionDocumentLength;
        prop.setProperty("collectionDocumentLength", String.valueOf(collectionDocumentLength));
        updateProperties();
    }

    /**
     * Set the number of entries in the lexicon
     * @param lexiconSize
     */
    public void setLexiconSize(int lexiconSize) {
        this.lexiconSize = lexiconSize;
        prop.setProperty("lexiconSize", String.valueOf(lexiconSize));
        updateProperties();
    }

    /**
     * Stores the statistics on disk
     */
    private void updateProperties() {
        try {
            prop.store(Files.newOutputStream(Paths.get(path)), null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
