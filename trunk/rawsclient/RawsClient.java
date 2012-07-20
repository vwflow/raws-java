/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package rawsclient;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HTTP;
import sun.misc.BASE64Encoder;


//class RawsException extends Exception 
//{
//    private int status_code = 0;
//
//    public RawsException(String msg) {
//        super(msg);
//    }
//    
//    public RawsException(int status, String msg) {
//        super(msg);
//        status_code = status;
//    }
//    
//    public int get_status_code() { return status_code; }
//}

/**
 *
 * @author bruno
 */
public class RawsClient {
    
    // set logger from your client application
    private final static Logger LOGGER = Logger.getLogger("SlideCapture");

    private String username;
    private String password;
    private String cdn;
    private String user_agent_name = "RawsClient.java";
    private Boolean ssl = false;
    private Boolean use_dev_server = false;
    
    private static final int RASS = 1;
    private static final int META = 2;
    private static final int RATS = 3;
    private static final int RAMS = 4;
    private static final int RASE = 5;

    public RawsClient(String username, String password, String cdn) 
    {
        this.username = username;
        this.password = password;
        this.cdn = cdn;
    }
    
    public void setUsername(String username) {
        this.username = username;
    }
    
    public void setPassword(String password) {
        this.password = password;
    }

    public void setCdn(String cdn) {
        this.cdn = cdn;
    }

    public void setUserAgentName(String user_agent_name) {
        this.user_agent_name = user_agent_name;
    }
    
    public void setSsl(Boolean ssl) {
        this.ssl = ssl;
    }
    
    public void useDevServer(Boolean use_dev_server) {
        this.use_dev_server = use_dev_server;
    }

    // Get the content from a HttpResponse (or any InputStream) as a String
    // StringBuilder can be converted to a normal String with the .toString() method.
    private StringBuilder inputStreamToString(InputStream is) throws IOException 
    {
        String line = "";
        StringBuilder total = new StringBuilder();
    
        // Wrap a BufferedReader around the InputStream
        BufferedReader rd = new BufferedReader(new InputStreamReader(is));

        // Read response until the end
        while ((line = rd.readLine()) != null) { 
            total.append(line); 
        }
    
        // Return full string
        return total;
    }    
    
    public Map<String, String> getParamsFromEntry(Map<String, Object> entryObj)
    {
        Map<String, Object> entry = (Map<String, Object>) entryObj.get("entry");
        Map<String, Object> content = (Map<String, Object>) entry.get("content");
        return (Map<String, String>) content.get("params");
    }

    public Map<String, String> getParamsFromFeedEntry(Map<String, Object> entry)
    {
        Map<String, Object> content = (Map<String, Object>) entry.get("content");
        return (Map<String, String>) content.get("params");
    }

    public Iterator<Object> getEntriesIterFromFeed(Map<String, Object> dirFeed) {
        Map<String, Object> entryObj = (Map<String, Object>) dirFeed.get("feed");
        List<Object> entries = (List<Object>) entryObj.get("entry");
        return entries.iterator();
    }
    
    public static String lstrip(String str, String remove) {
        if ( str.isEmpty() || remove.isEmpty() ) {
            return str;
        }
        while (str.indexOf(remove) == 0) {
            str = str.substring(remove.length());
            if ( str.isEmpty() ) {
                break;
            }
        }
        return str;
    }
    
    public static String rstrip(String str, String remove) {
        if ( str.isEmpty() || remove.isEmpty() ) {
            return str;
        }
        while (str.endsWith(remove)) {
            str = str.substring(0, str.length() - remove.length());
            if ( str.isEmpty() ) {
                break;
            }
        }
        return str;
    }
    
    public String getUrl(int service_id, String path, String querystr) 
    {
        String server = "http://";
        if (this.ssl) {
            server = "https://";
        }
        switch (service_id) {
            case RawsClient.RASS:
                server = server + "rass." + this.cdn + ".rambla.be/";
                break;
            case RawsClient.META:
                if (this.use_dev_server) {
                    server = server + "meta.meta03.rambla.be/";
                }
                else {
                    server = server + "meta.meta01.rambla.be/";
                }
                break;
            case RawsClient.RATS:
                if (this.use_dev_server) {
                    server = server + "rats.enc02.rambla.be/";
                }
                else {
                    server = server + "rats.enc01.rambla.be/";
                }
                break;
            case RawsClient.RASE:
                if (this.use_dev_server) {
                    server = server + "rase.str03.rambla.be/";
                }
                else {
                    server = server + "rase.str01.rambla.be/";
                }
                break;
            case RawsClient.RAMS:
                if (this.use_dev_server) {
                    server = server + "rams.mon02.rambla.be/";
                }
                else {
                    server = server + "rams.mon01.rambla.be/";
                }
                break;
        }
        server = server + path;
        if (querystr != null) {
            server = server + "?" + querystr;
        }
        
        return server;
    }
    
    private Map<String, Object> GET(int service_id, String uri, String querystr) throws HttpResponseException, IOException 
    {
        String url = getUrl(service_id, uri, querystr);
    
        HttpGet httpGet = new HttpGet(url);
        httpGet.addHeader("Accept", "application/json");

        return exec_request((HttpRequestBase) httpGet);
    }
    
    private Map<String, Object> POST(int service_id, String uri, Map<String, Object> dataMap, String querystr) throws HttpResponseException, IOException
    {
        String url = getUrl(service_id, uri, querystr);

        ObjectMapper mapper = new ObjectMapper();
        StringEntity entity = new StringEntity(mapper.writeValueAsString(dataMap), HTTP.UTF_8);
        
        HttpPost httpPost = new HttpPost(url);
        entity.setContentType("application/json");
        httpPost.setEntity(entity);
        httpPost.addHeader("Accept", "application/json");
        
        return exec_request((HttpRequestBase) httpPost);
    }

    private Map<String, Object> PUT(int service_id, String uri, String slug, String filepath, String querystr, int max_bps, int chunk_size) throws HttpResponseException, IOException
    {
        String url = getUrl(service_id, uri, querystr);

        HttpPut httpPut = new HttpPut(url);  //-X PUT
        File file = new File(filepath);
        
        if (max_bps > 0) {
            int chunkSize = 2048;
            if (chunk_size > 2048) {
                chunkSize = chunk_size;
            }
            httpPut.setEntity(new ThrottledEntity(file, "video/*", max_bps, chunkSize, 200));
        }
        else {
            httpPut.setEntity(new InputStreamEntity(new FileInputStream(file), file.length()));
            httpPut.addHeader("Content-Type", "video/*");
        }

        httpPut.addHeader("Accept", "application/json");
        httpPut.addHeader("Slug", slug);

        return exec_request((HttpRequestBase) httpPut);
    }


    private Map<String, Object> exec_request(HttpRequestBase httpMethod) throws HttpResponseException, IOException
    {
        DefaultHttpClient httpClient = new DefaultHttpClient();
        httpClient.getParams().setParameter("http.useragent", this.user_agent_name);
        
        BASE64Encoder enc = new sun.misc.BASE64Encoder();
        String userpassword = this.username + ":" + this.password;
        String encodedAuthorization = enc.encode( userpassword.getBytes() );
        httpMethod.addHeader("Authorization", "Basic "+ encodedAuthorization);

        HttpResponse response = httpClient.execute(httpMethod);
        if (response.getStatusLine().getStatusCode() > 299) {
            throw new HttpResponseException(response.getStatusLine().getStatusCode(), inputStreamToString(response.getEntity().getContent()).toString());
        }
        ObjectMapper mapper2 = new ObjectMapper();
        Object responseObject = mapper2.readValue(response.getEntity().getContent(), Object.class);
        httpMethod.releaseConnection();

        return (Map<String, Object>) responseObject;
    }
    
    public Map<String, Object> rass_getDir(String cdn_path, String querystr) throws IOException
    {
        String uri = "dir/" + lstrip(cdn_path, "/");
        
        return GET(RawsClient.RASS, uri, querystr);
    }
    
    public Map<String, Object> rass_putItem(String cdn_path, String cdn_filename, String filepath) throws IOException
    {
        String uri = "item/" + lstrip(cdn_path, "/");
        
        return PUT(RawsClient.RASS, uri, cdn_filename, filepath, null, 0, 0);
    }

    public Map<String, Object> rass_putItem(String cdn_path, String cdn_filename, String filepath, int maxBps, int chunkSize) throws IOException
    {
        String uri = "item/" + lstrip(cdn_path, "/");
        
        return PUT(RawsClient.RASS, uri, cdn_filename, filepath, null, maxBps, chunkSize);
    }
    
    
    public Map<String, Object> meta_postWslide(String webcastID, String path, String timestamp, String offset) throws UnsupportedEncodingException, IOException
    {
        String uri = "wslide/" + this.username + "/" + webcastID + "/";
        
	Map<String, Object> entryMap = new HashMap<String, Object>();
	Map<String, Object> entryInMap = new HashMap<String, Object>();
	Map<String, Object> contentInMap = new HashMap<String, Object>();
	Map<String, String> paramsInMap = new HashMap<String, String>();
	paramsInMap.put("webcast_id", webcastID);
	paramsInMap.put("path", path);
	paramsInMap.put("timestamp", timestamp);
	paramsInMap.put("offset", offset);
	contentInMap.put("params", paramsInMap);
	entryInMap.put("content", contentInMap);
	entryMap.put("entry", entryInMap);
        
        return POST(RawsClient.META, uri, entryMap, null);
    }
    
            
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException 
    {
        try {

            RawsClient rawsClient = new RawsClient("bruno03", "K0nijn;;", "cdn03");
            rawsClient.useDevServer(true);
            
//            Map<String, Object> dirFeed = rawsClient.rass_getDir("/java", null);
//            Iterator<Object> entriesIter = rawsClient.getEntriesIterFromFeed(dirFeed);
//            while ( entriesIter.hasNext() ){
//                Map<String, String> params = rawsClient.getParamsFromFeedEntry((Map<String, Object>) entriesIter.next());
//                String cdn_path = params.get("path");
//                String kind = params.get("kind");
//            }
            
            Map<String, Object> itemEntry = rawsClient.rass_putItem("/java", "slide.jpg", "/Users/bruno/Tmp/slide_capture/slides/slide_1342628213.jpg");
            Map<String, String> itemParams = rawsClient.getParamsFromEntry(itemEntry);
            String cdn_path = itemParams.get("path");
            System.out.println("upload done, cdn path = " + cdn_path);


//            Map<String, Object> wslideEntry = rawsClient.meta_postWslide("106", "/hava/slide.jpg", "70605559", "40");
//            Map<String, String> wslideParams = rawsClient.getParamsFromEntry(wslideEntry);
//            String id = wslideParams.get("id");
//            String timestamp = wslideParams.get("timestamp");
//            String path = wslideParams.get("path");
        }
        catch (Exception ex)
        {
            LOGGER.severe("Exception raised by RawsClient:doRequest() : " + ex);
        }
    
    }
    
}
        
// OLD METHOD WITHOUT USING APACHE HttpClient
//    private Object doRequest(String url, String method, Map<String, Object> dataMap, String filepath, Boolean decode, Map<String, String> extraHeadersMap)
//    {
//        URL urlObj;
//        HttpURLConnection connection = null;
//        Object responseObject = null;
//        
//        try 
//        {
//            //Create connection
//            urlObj = new URL(url);
//            connection = (HttpURLConnection)urlObj.openConnection();
//            connection.setRequestMethod(method);
//            
//            if (filepath == null) { 
//                connection.setRequestProperty("Content-Type", "application/json");
//            }
//            else { // if we're uploading a file, set the SLUG header + Content-Length of the payload
//                connection.setRequestProperty("Content-Type", "video/*");
//            }
//            
//            // unless decode is False, we want the response to be encoded in to json
//            if (decode) {
//                connection.setRequestProperty("Accept", "application/json");
//            }
//
//            BASE64Encoder enc = new sun.misc.BASE64Encoder();
//            String userpassword = this.username + ":" + this.password;
//            String encodedAuthorization = enc.encode( userpassword.getBytes() );
//            connection.setRequestProperty("Authorization", "Basic "+ encodedAuthorization);
//    //        connection.setRequestProperty("Content-Length", "" + 
//    //                Integer.toString(urlParameters.getBytes().length));
//
//            if (extraHeadersMap != null) {
//                Set set = extraHeadersMap.entrySet();
//                Iterator i = set.iterator();
//                while(i.hasNext()){
//                    Map.Entry me = (Map.Entry)i.next();
//                    connection.setRequestProperty((String)me.getKey(), (String)me.getValue());
//                }
//            }
//            connection.setUseCaches(false);
//            connection.setDoInput(true);
//            connection.setDoOutput(true);
//            
//            ObjectMapper mapper = new ObjectMapper();
//            mapper.writeValue(connection.getOutputStream(), dataMap);
//
//            // SEND REQUEST + GET RESPONSE
//            InputStream is = connection.getInputStream();
//            responseObject = mapper.readValue(is, Object.class);
//            
//        }
//        catch (Exception ex)
//        {
//            LOGGER.log(Level.SEVERE, "Exception raised by RawsClient:doRequest() : {0}", ex);
//        }
//        finally 
//        {
//            if(connection != null) {
//                connection.disconnect(); 
//            }
//        }
//            
//        return responseObject;
//    }
        
