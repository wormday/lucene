package org.apache.lucene.index;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.JavaLoggingInfoStream;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.compress.LZ4;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;

public class Test {
    public IndexWriter indexWriter;
    public IndexReader indexReader;
    public Directory indexDirectory;
    public IndexSearcher searcher;

    public void open() throws IOException {
        indexDirectory = FSDirectory.open(Paths.get("D:\\data\\lucene\\test"));
        IndexWriterConfig conf = new IndexWriterConfig();
        PrintStream ps = new PrintStream("D:\\data\\lucene\\test\\log.txt");
        conf.openMode = IndexWriterConfig.OpenMode.CREATE_OR_APPEND;
        conf.useCompoundFile=false;
        conf.setInfoStream(new PrintStreamInfoStream(ps));
        indexWriter = new IndexWriter(indexDirectory, conf);
    }

    public void run() throws IOException, InterruptedException {
        open();
        // this.indexWriter.deleteAll();
        // this.indexWriter.commit();
//        if(Arrays.asList(indexDirectory.listAll()).contains("abc.abc")){
//            indexDirectory.deleteFile("abc.abc");
//        }
//        IOContext ioCtx = new IOContext();
//        try(IndexOutput output = indexDirectory.createOutput("abc.abc", ioCtx)){
//            // [长度|VInt][utf-8]
//            output.writeString("孙");
//            output.writeByte(Byte.parseByte("0"));
//            output.writeInt(Integer.MAX_VALUE);
//            output.writeVInt(Integer.MAX_VALUE);
//            System.out.println(Integer.MAX_VALUE);
//        }

        Document doc1 = new Document();
        doc1.add(new StringField("title", "doc1 title1", Field.Store.YES));
        doc1.add(new StringField("title", "doc1 title2", Field.Store.YES));
        doc1.add(new StringField("title", "doc1 title3", Field.Store.YES));

        this.indexWriter.addDocuments(List.of(doc1));
        this.indexWriter.flush();


//        Thread thread = new Thread(this::update);
//        thread.start();
//        thread.join();
        this.indexWriter.commit();

        indexReader = DirectoryReader.open(indexDirectory);
        searcher = new IndexSearcher(indexReader);
        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 100);
        System.out.println(String.format("doc num:%s",topDocs.totalHits.value));
        for(ScoreDoc doc : topDocs.scoreDocs){
            Document document = searcher.doc(doc.doc);
            System.out.println(String.format("%s,%s",doc.doc,document.get("title")));
        }

        close();
    }

    public void update() {
        try {
            Document doc1 = new Document();
            doc1.add(new StringField("title", "def", Field.Store.YES));
            long l = this.indexWriter.updateDocument(new Term("title", "abc"), doc1);
            System.out.println(String.format("update document:%s", l));
        } catch (Exception ex) {
            System.out.println(String.format("更新文档失败"));
        }
    }

    public void close() throws IOException {
        indexWriter.close();
        if(indexReader!=null) {
            indexReader.close();
        }
        indexDirectory.close();
    }

    public static void main(String[] args) throws IOException {
        try {
            Test test = new Test();
            test.run();
        } catch (Exception ex) {
            System.out.println(ex.toString());
            StackTraceElement[] stackTrace = ex.getStackTrace();
            for (StackTraceElement e : stackTrace) {
                System.out.println(e.toString());
            }
        }
    }
}


