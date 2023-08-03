package it.unipi.dii.mircv.yase.structures;

/**
 * A simple class to describe a document of the collection
 */
public class DocumentDescriptor implements Comparable<DocumentDescriptor>, IdentifiableObject<Integer> {
    private int docid;
    private String docno;
    private int doclen;

    /**
     * Creates a document descriptor
     * @param docId
     * @param docNo
     * @param docLen
     */
    public DocumentDescriptor(final int docId, final String docNo, final int docLen) {
        this.docid = docId;
        this.docno = docNo;
        this.doclen = docLen;
    }

    public int getDocId() {
        return docid;
    }

    public String getDocNo() {
        return docno;
    }

    public int getDocLen() {
        return doclen;
    }

    @Override
    public Integer getID() {
        return docid;
    }

    @Override
    public String toString() {
        return "DocumentDescriptor{" +
                "docid=" + docid +
                ", docno='" + docno + '\'' +
                ", doclen=" + doclen +
                '}';
    }

    /**
     * Compares two descriptors by increasing docid
     * @param that the object to be compared.
     */
    @Override
    public int compareTo(final DocumentDescriptor that) {
        return Integer.compare(this.docid, that.docid);
    }
}
