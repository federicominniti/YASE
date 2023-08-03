package it.unipi.dii.mircv.yase.structures;

/**
 * Needed to explicit the fact that each Entry object in an ArrayFile is identified and
 * ordered through one of its fields, called ID.
 * @param <K> the type of the field identifying the object
 */
public interface IdentifiableObject<K> {
    K getID();
}