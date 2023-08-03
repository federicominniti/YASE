package it.unipi.dii.mircv.yase.util.preprocessing;

import it.unipi.dii.mircv.yase.util.Configuration;
import opennlp.tools.stemmer.snowball.SnowballStemmer;
import org.apache.commons.lang3.tuple.Pair;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Singleton class with a text preprocessing utility methods
 */
public class TextPreprocessing {
    // Array of stopword loaded from a stop words file
    private static String[] stopwords;

    // Singleton text preprocessor's instance
    private static TextPreprocessing textPreprocessor;

    private TextPreprocessing() {
        stopwords = null;
        try {
            List<String> tmp = Files.readAllLines(Paths.get(Configuration.stopwordsFile));
            stopwords = tmp.toArray(new String[tmp.size()]);
            Arrays.sort(stopwords);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Getter of the text preprocessor instance
     * @return
     */
    public static TextPreprocessing getInstance() {
        if (textPreprocessor == null) {
            textPreprocessor = new TextPreprocessing();
        }
        return textPreprocessor;
    }

    /**
     * Method that given a document tokenize it and return a pair of: docno and a list of the preprocessed tokens
     * @param document
     * @return
     */
    public Pair<String, List<String>> tokenize(String document) {
        String[] content = document.split("\t");

        return Pair.of(
                content[0],
                preprocessText(content[1]));

    }

    /**
     * Method that execute all the preprocessing steps on a text
     * @param text
     * @return
     */
    public List<String> preprocessText(String text){
        return Arrays.stream(
                                // Filter only ASCII characters
                        text.replaceAll("[^\\p{ASCII}]|\\p{Punct}", " ")
                                // Put all in lower case
                                .toLowerCase()
                                // Tokenization
                                .split("\\s+"))
                // Remove too long terms
                .filter(term -> term.length() < Configuration.termLength)
                // Remove stopwords
                .filter(this::isNotStopword)
                // Remove empty tokens
                .filter(term -> !term.equals(""))
                // Stem tokens
                .map(this::stemWord)
                .collect(Collectors.toList());
    }

    /**
     * Method that execute all the preprocessing steps on a text and return distinct tokens
     * @param text
     * @return
     */
    public List<String> preprocessTextDistinct(String text){
        return Arrays.stream(
                        text.replaceAll("[^\\p{ASCII}]|\\p{Punct}", " ")
                                .toLowerCase()
                                .split("\\s+"))
                .filter(term -> term.length() < Configuration.termLength)
                .filter(this::isNotStopword)
                .filter(term -> !term.equals(""))
                .map(this::stemWord)
                // Remove duplicates tokens
                .distinct()
                .collect(Collectors.toList());
    }

    /**
     * Method to check if the word passed is a stopwords or not
     * @param term
     * @return
     */
    private boolean isNotStopword(String term) {
        if (!Configuration.useStopwords)
            return true;

        int indexOf = Arrays.binarySearch(stopwords, term);
        return (indexOf < 0);
    }

    /**
     * Method to stem a passed word
     * @param term
     * @return
     */
    private String stemWord(String term) {
        if (!Configuration.useStemming)
            return term;

        return new SnowballStemmer(SnowballStemmer.ALGORITHM.ENGLISH).stem(term).toString();
    }
}
