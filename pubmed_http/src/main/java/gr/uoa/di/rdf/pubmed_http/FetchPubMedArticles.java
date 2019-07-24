package gr.uos.di.rdf.pubmed_http;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.DocumentType;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class FetchPubMedArticles implements Runnable {

    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(FetchPubMedArticles.class.getSimpleName());

    private final String USER_AGENT = "Mozilla/5.0";
    private final String BASE_URI = "https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi";
    private final String E_UTIL_BASE_URI = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/";
    private final String E_FETCH = "efetch.fcgi?";
    private final String E_POST = "epost.fcgi?";
    private static final String PUBMED = "pubmed";
    private final static String RESOURCE_DIR = ClassLoader.getSystemResource(".").getPath();
    private static final String INPUT_DIR = RESOURCE_DIR + "input/";
    private static String UIDS_FILE = "";
    private static String OUTPUT_DIR = RESOURCE_DIR + "output/";
    private static String erroneousPMIDsFile = "";
    private static int POST_BATCH_SIZE = 400;
    //private static final int FETCH_BATCH_SIZE = 400;
    private static final String API_KEY_TIOA = "96224f21d4167737bcd710073a3564115907";
    private static final String API_KEY_ALEX = "699b4b70c99abf960bba84de689b38ffc709";
    private static final String API_KEY = API_KEY_ALEX;
    private static List<String> pmidList = null;
    //private static List<String> pmidFetchedList = new ArrayList<>();
    private static int ThreadsMaxNo = 3;

    class EntrezHistoryUIDs {

        // DATA MEMBERS
        private String db;
        private String query_key;
        private String webenv;

        // CONSTRUCTOR
        EntrezHistoryUIDs(String db, String query_key, String webenv) {
            this.db = db;
            this.query_key = query_key;
            this.webenv = webenv;
        }

        // DATA ACCESSORS
        public String getDb() {
            return db;
        }

        public String getQuery_key() {
            return query_key;
        }

        public String getWebenv() {
            return webenv;
        }

        // METHODS
        private String getSimpleDescription() {
            return "db:" + db + ", "
                    + "query_key:" + query_key + ", "
                    + "webenv:" + webenv;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "["
                    + getSimpleDescription() + "]";
        }
    }

    // DATA MEMBERS
    private List<String> partialPMIDList;
    private int postStart, postEnd;
    private String pmids;
    private EntrezHistoryUIDs ehUIDs;

    public FetchPubMedArticles(int postStart, int postEnd) {
        this.postStart = postStart;
        this.postEnd = postEnd;
        this.partialPMIDList = pmidList.subList(postStart, postEnd + 1); // BEWARE! subList is [postStart, postEnd+1)
        this.pmids = String.join(",", this.partialPMIDList);
        this.ehUIDs = null;
        logger.debug(this.toString());
    }

    // METHODS
    private String getSimpleDescription() {
        return "postStart:" + postStart + ", "
                + "postEnd:" + postEnd;
    }

    private String getFullDescription() {
        return this.getSimpleDescription() + ", "
                + "ehUIDs:" + ehUIDs.toString() + ", "
                + "pmids:" + pmids;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "["
                + getSimpleDescription() + "]";
    }

    public String toString(boolean fullDesc) {
        if (!fullDesc) {
            return toString();
        } else {
            return getClass().getSimpleName() + "["
                    + getFullDescription() + "]";
        }
    }

    @Override
    public void run() {
        List<String> fetchedSubList = new ArrayList<>();
        Set<String> partialPMIDset = new HashSet<>(partialPMIDList);
        Set<String> fetchedSubset;
        try {
            logger.info(Thread.currentThread().getName() + " |--> GET E-Post (" + postStart + ", " + postEnd + ")");
            this.ehUIDs = this.sendGet_E_Post_UIDList(PUBMED, API_KEY, this.pmids);
            logger.debug(this.toString(true));
            Thread.sleep(1100);
            logger.info(Thread.currentThread().getName() + " |----> GET E-Fetch (" + postStart + ", " + postEnd + ")");
            fetchedSubList.addAll(this.sendGet_E_Fetch_FromHistory(this.ehUIDs, API_KEY, Integer.toString(0), Integer.toString(postEnd - postStart + 1), "XML", OUTPUT_DIR));
            Thread.sleep(2100);
            if (partialPMIDList.size() != fetchedSubList.size()) {
                logger.error("ERROR: [" + postStart + ", " + postEnd + "] - Fetched sublist has size " + fetchedSubList.size() + ", while posted sublist has size " + partialPMIDList.size());
                // find the PMIDs which we did not receive any response for
                fetchedSubset = new HashSet<>(fetchedSubList);
                partialPMIDset.removeAll(fetchedSubset);
                synchronized (this) { // make sure that only one thread at a time appends to the file
                    Files.write(Paths.get(erroneousPMIDsFile), new ArrayList<String>(partialPMIDset), StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                }
            }
            //pmidFetchedList.addAll(fetchedSubList);
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        logger.debug(System.getProperty("java.class.path"));
        // read arguments
        if (args[0].equalsIgnoreCase("-input")) { // read input file with UIDs
            UIDS_FILE = INPUT_DIR + args[1];
            logger.debug("Resources Input path through ClassLoader is : " + UIDS_FILE);
            pmidList = getPMIDsList(UIDS_FILE);
        }
        if (args[2].equalsIgnoreCase("-batch")) { // read ePost batch size
            POST_BATCH_SIZE = Integer.parseInt(args[3]);
        }
        if (args[4].equalsIgnoreCase("-threads")) { // read no of threads to use
            ThreadsMaxNo = Integer.parseInt(args[5]);
        }
        if (args[6].equalsIgnoreCase("-outdir")) { // read output dir, must end in /
            OUTPUT_DIR = args[7];
            erroneousPMIDsFile = OUTPUT_DIR + "erroneousPMIDS.txt";
        }

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(ThreadsMaxNo);
        int post_batch_len = POST_BATCH_SIZE;
        for (int i = 0; i < pmidList.size(); i += POST_BATCH_SIZE) {
            if (pmidList.size() - i < post_batch_len) {
                post_batch_len = pmidList.size() - i;
            }
            executor.execute(new FetchPubMedArticles(i, i + post_batch_len - 1));
        }
        executor.shutdown();
    }

    private static ArrayList<String> getPMIDsList(String uidFilePath) throws FileNotFoundException {
        Scanner s = new Scanner(new File(uidFilePath));
        ArrayList<String> pmid_list = new ArrayList<String>();
        while (s.hasNext()) {
            pmid_list.add(s.next());
        }
        s.close();
        return pmid_list;
    }

    // UIDs : comma seperated list of UIDs
    // retmode : XML or smthg...
    private EntrezHistoryUIDs sendGet_E_Post_UIDList(String db, String api_key, String UIDs) throws Exception {
        String url = E_UTIL_BASE_URI + E_POST
                + "db=" + db
                + "&id=" + UIDs
                + "api_key=" + api_key;

        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.setRequestMethod("GET");
        //add request header
        con.setRequestProperty("User-Agent", USER_AGENT);

        int responseCode = 0;
        try {
            responseCode = con.getResponseCode();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        logger.debug("\nSending 'GET' request to URL : " + url);
        logger.debug("Response Code : " + responseCode);

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));

        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        //print result
        logger.debug(response.toString());

        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse(new ByteArrayInputStream(response.toString().getBytes(StandardCharsets.UTF_8)));
        doc.getDocumentElement().normalize();

        //String ftpUrl = null;
        logger.debug("Root element :" + doc.getDocumentElement().getNodeName() + " , type =  " + doc.getDocumentElement().getNodeType());

        String querykey = null;
        String webenv = null;
        NodeList children = doc.getDocumentElement().getChildNodes();
        Node node = null;
        for (int n = 0; n < children.getLength(); n++) {
            node = children.item(n);

            if (node.getNodeType() == Node.ELEMENT_NODE) {
                if ("QueryKey".equalsIgnoreCase(node.getNodeName())) {
                    querykey = node.getTextContent();
                } else if ("WebEnv".equalsIgnoreCase(node.getNodeName())) {
                    webenv = node.getTextContent();
                }
            }
        }

        return new EntrezHistoryUIDs(db, querykey, webenv);
    }

    // UIDs : comma seperated list of UIDs
    // retmode : XML or smthg...
    private List<String> sendGet_E_Fetch_FromHistory(EntrezHistoryUIDs ehUIDs, String api_key, String retstart, String retmax, String retmode, String targetDir) throws Exception {
        String url = E_UTIL_BASE_URI + E_FETCH
                + "db=" + ehUIDs.getDb()
                + "&query_key=" + ehUIDs.getQuery_key()
                + "&WebEnv=" + ehUIDs.getWebenv()
                + "api_key=" + api_key
                + "&retstart=" + retstart
                + "&retmax=" + retmax
                + "&retmode=" + retmode;

        List<String> fetchedPMIDs = new ArrayList<>();
        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.setRequestMethod("GET");
        //add request header
        con.setRequestProperty("User-Agent", USER_AGENT);

        int responseCode = con.getResponseCode();
        logger.debug("\nSending 'GET' request to URL : " + url);
        logger.debug("Response Code : " + responseCode);

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));

        String inputLine, headerXML, headerDTD;
        StringBuffer response = new StringBuffer();

        // read XML version header
        headerXML = in.readLine();
        response.append(headerXML);
        // read DTD version header
        headerDTD = in.readLine();
        response.append(headerDTD);

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        //print result
        logger.debug(response.toString());

        // create DOM Document to help parse HTTP Get response
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document pubmedArticleSetDoc = dBuilder.parse(new ByteArrayInputStream(response.toString().getBytes(StandardCharsets.UTF_8)));
        pubmedArticleSetDoc.getDocumentElement().normalize();

        logger.debug("Root element :" + pubmedArticleSetDoc.getDocumentElement().getNodeName() + " , type =  " + pubmedArticleSetDoc.getDocumentElement().getNodeType());

        // create and configure DOM Transformer to create XML file for each <PubmedArticle>
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
        transformer.setOutputProperty(OutputKeys.METHOD, "xml");

        String pubmedArticleID = null;
        DOMSource pubmedArticleSource = null;
        StreamResult pubmedArticleXMLStream = null;
        Document pubmedArticleDoc = null;
        DOMImplementation pubmedArticleDOMImpl = null;

        // if target directory does not exist create it
        new File(targetDir).mkdirs();

        // get the <PubmedArticle> nodes under <PubmedArticleSet>
        NodeList pubmedArticleNodeLst = pubmedArticleSetDoc.getDocumentElement().getChildNodes();
        Node pubmedArticleNode = null;
        Element pubmedArticleSetRootElement = null;
        // iterate over each <PubmedArticle> node and create an XML file
        for (int nodeIdx = 0; nodeIdx < pubmedArticleNodeLst.getLength(); nodeIdx++) {
            pubmedArticleNode = pubmedArticleNodeLst.item(nodeIdx);
            if (pubmedArticleNode.getNodeType() == Node.ELEMENT_NODE) {
                if ("PubmedArticle".equalsIgnoreCase(pubmedArticleNode.getNodeName())) {
                    // retrieve PMID of pubmedArticleNode
                    Element pubmedArticleElement = (Element) pubmedArticleNode;
                    pubmedArticleID = pubmedArticleElement.getElementsByTagName("PMID").item(0).getTextContent();
                    // add retrieved PMID to fetchedPMIDs list
                    fetchedPMIDs.add(pubmedArticleID);
                    // create serialized XML form of pubmedArticleNode (new DOM Document)
                    pubmedArticleDoc = dBuilder.newDocument();
                    // create pubmedArticleSetRootElement 
                    pubmedArticleSetRootElement = pubmedArticleDoc.createElement("PubmedArticleSet");
                    // attach pubmedArticleSetRootElement element to pubmedArticleDoc
                    pubmedArticleDoc.appendChild(pubmedArticleSetRootElement);
                    // import pubmedArticleNode to pubmedArticleDoc as pubmedArticleCloneNode
                    Node pubmedArticleCloneNode = pubmedArticleDoc.importNode(pubmedArticleNode, true);
                    // Transfer ownership of the new node pubmedArticleCloneNode into the destination document
                    // by attaching it to the root node pubmedArticleSetRootElement
                    pubmedArticleSetRootElement.appendChild(pubmedArticleCloneNode);
                    // Make the new node an actual item in the target document
                    // pubmedArticleDoc.getDocumentElement().appendChild(pubmedArticleCloneNode);

                    pubmedArticleSource = new DOMSource(pubmedArticleDoc);
                    pubmedArticleXMLStream = new StreamResult(new OutputStreamWriter(
                            new FileOutputStream(new File(targetDir + "//" + pubmedArticleID + ".xml")), "UTF-8"));

                    pubmedArticleDOMImpl = pubmedArticleDoc.getImplementation();
                    DocumentType doctype = pubmedArticleDOMImpl.createDocumentType("PubmedArticleSet",
                            "-//NLM//DTD PubMedArticle, 1st January 2019//EN",
                            "https://dtd.nlm.nih.gov/ncbi/pubmed/out/pubmed_190101.dtd");
                    transformer.setOutputProperty(OutputKeys.DOCTYPE_PUBLIC, doctype.getPublicId());
                    transformer.setOutputProperty(OutputKeys.DOCTYPE_SYSTEM, doctype.getSystemId());
                    transformer.transform(pubmedArticleSource, pubmedArticleXMLStream);
                }
            }
        }
        return fetchedPMIDs;
    }

}
