package edu.cmeiners;

import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.document.TextDocument;
import de.l3s.boilerpipe.extractors.ArticleExtractor;
import de.l3s.boilerpipe.sax.BoilerpipeSAXInput;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


public class Main {

    final static String DATA_PATH = "/mnt/claas/data/";
    final static String ARTICLES_BASE_PATH = DATA_PATH + "/raw/articles/";
    final static String TEXT_BASE_PATH = DATA_PATH + "/interim/articles_text/";


    public static void main(String[] args) {
        // Create the text dir if it does not exist yet
        //noinspection ResultOfMethodCallIgnored
        new File(TEXT_BASE_PATH).mkdirs();

        ArticleExtractor extractor = new ArticleExtractor();

        try {
            AtomicInteger count = new AtomicInteger();
            Files.walk(Paths.get(ARTICLES_BASE_PATH))
                    .filter(Files::isRegularFile)
                    .forEach(path -> {
                        String articleFilePath = path.toString();
                        String articlePath = articleFilePath.substring(0, articleFilePath.lastIndexOf(File.separator));
                        if (articleFilePath.endsWith(".gzip")) {
                            try {
                                String textPath = TEXT_BASE_PATH + Paths.get(ARTICLES_BASE_PATH).relativize(Paths.get(articlePath));
                                String textFilePath = textPath + "/" + path.getFileName().toString(); // same filename
                                File textFile = new File(textFilePath);
                                if (!textFile.isFile() || textFile.length() == 20) {
                                    try {
                                        // Only if the text hasn't been extracted yet
                                        TextDocument input = readTextDocument(articleFilePath);
                                        extractor.process(input);
                                        String text = input.getContent();

                                        //noinspection ResultOfMethodCallIgnored
                                        new File(textPath).mkdirs();
                                        writeText(text, textFilePath);
                                    } catch (StackOverflowError e) {
                                        System.out.println(articleFilePath);
                                    }
                                }
                                int currentCount = count.getAndIncrement();
                                if (currentCount % 1000 == 0) {
                                    System.out.println(currentCount);
                                }
                            } catch (IOException | SAXException | BoilerpipeProcessingException e) {
                                e.printStackTrace();
                            }
                        }
                    });

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeText(String text, String textFilePath) throws IOException {
        Writer writer = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(textFilePath)), StandardCharsets.UTF_8);
        writer.write(text);
        writer.close();
    }

    private static TextDocument readTextDocument(String articleFilePath) throws IOException, SAXException, BoilerpipeProcessingException {
        return new BoilerpipeSAXInput(new InputSource(new InputStreamReader(new GZIPInputStream(new FileInputStream(articleFilePath))))).getTextDocument();
    }
}