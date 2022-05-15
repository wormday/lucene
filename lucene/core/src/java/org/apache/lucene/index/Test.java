package org.apache.lucene.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

public class Test {
    public IndexWriter indexWriter;
    public IndexReader indexReader;
    public Directory indexDirectory;

    public void open() throws IOException {
        indexDirectory = FSDirectory.open(Paths.get("D:\\data\\lucene\\test"));
        IndexWriterConfig conf = new IndexWriterConfig();
        conf.openMode = IndexWriterConfig.OpenMode.CREATE_OR_APPEND;
        indexWriter = new IndexWriter(indexDirectory, conf);
        indexReader = DirectoryReader.open(indexDirectory);
    }

    public void run() throws IOException {
        open();

        Document doc1 = new Document();
        doc1.add(new StringField("id", "cat", Field.Store.NO));
        doc1.add(new StringField("id", "dog", Field.Store.NO));
        long docId = indexWriter.addDocument(doc1);
        long commitId = indexWriter.commit();
        System.out.printf("docId:%s,commitId:%s%n", docId, commitId);


        Document doc2 = new Document();
        doc2.add(new StringField("id", "cat", Field.Store.NO));
        doc2.add(new StringField("id", "dog", Field.Store.NO));
        docId = indexWriter.addDocument(doc2);
        commitId = indexWriter.commit();
        System.out.printf("docId:%s,commitId:%s%n", docId, commitId);

        int maxDoc = indexWriter.getDocStats().maxDoc;
        int numDocs = indexWriter.getDocStats().numDocs;
        System.out.printf("maxDoc:%s,numDocs:%s%n", maxDoc, numDocs);


        long del = indexWriter.deleteDocuments(new Term("id", "cat"));
        System.out.printf("delId:%s%n", del);

        maxDoc = indexWriter.getDocStats().maxDoc;
        numDocs = indexWriter.getDocStats().numDocs;
        System.out.printf("maxDoc:%s,numDocs:%s%n", maxDoc, numDocs);

        close();
    }

    public void close() throws IOException {
        indexWriter.close();
        indexReader.close();
        indexDirectory.close();
    }

    public static void main(String[] args) throws IOException {
        try {
            Test test = new Test();
            test.run();
        } catch (Exception ex) {
            System.out.println(ex.toString());
        }
    }
}


