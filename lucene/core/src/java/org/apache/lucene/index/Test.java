package org.apache.lucene.index;

import org.apache.lucene.document.*;
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
        indexWriter = new IndexWriter(indexDirectory, conf);
        indexReader = DirectoryReader.open(indexDirectory);
    }

    public void run() throws IOException {
        open();
        for (int i = 0; i <= 10; i++) {
            Document doc1 = new Document();
            doc1.add(new StringField("id", "cat", Field.Store.NO));
            doc1.add(new IntPoint("id", 2));
            doc1.add(new TextField("title","This is Halloween, everybody make a scene",Field.Store.NO));
            long docId = indexWriter.addDocument(doc1);
            long commitId = indexWriter.commit();
            System.out.printf("docId:%s,commitId:%s%n", docId, commitId);
        }
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
            System.out.println(ex.getStackTrace());
            StackTraceElement[] stackTrace = ex.getStackTrace();
            for(StackTraceElement e:stackTrace){
                System.out.println(e.toString());
            }
        }
    }
}


