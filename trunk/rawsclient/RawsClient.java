/*
 * Copyright (C) 2012 rambla.eu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package rawsclient;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import sun.misc.BASE64Encoder;

/**
 *
 * @author bruno
 */
public class RawsClient {
    
    // set logger from your client application
    private static Logger LOGGER; 

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
        RawsClient.LOGGER = Logger.getLogger("RawsClient");
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
        String line;
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
    
    @SuppressWarnings("unchecked")
    public Map<String, String> getParamsFromEntry(Map<String, Object> entryObj) throws ClassCastException
    {
        Map<String, Object> entry = (Map<String, Object>) entryObj.get("entry");
        Map<String, Object> content = (Map<String, Object>) entry.get("content");
        return (Map<String, String>) content.get("params");
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> getParamsFromFeedEntry(Map<String, Object> entry) throws ClassCastException
    {
        Map<String, Object> content = (Map<String, Object>) entry.get("content");
        return (Map<String, String>) content.get("params");
    }

    @SuppressWarnings("unchecked")
    public Iterator<Object> getEntriesIterFromFeed(Map<String, Object> dirFeed) throws ClassCastException 
    {
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
    
    private Map<String, Object> GET(int service_id, String uri, String querystr) throws HttpResponseException, IOException, ClassCastException 
    {
        String url = getUrl(service_id, uri, querystr);
    
        HttpGet httpGet = new HttpGet(url);
        httpGet.addHeader("Accept", "application/json");

        return exec_request((HttpRequestBase) httpGet);
    }
    
    private Map<String, Object> POST(int service_id, String uri, Map<String, Object> dataMap, String querystr) throws HttpResponseException, IOException, ClassCastException
    {
        String url = getUrl(service_id, uri, querystr);

        ObjectMapper mapper = new ObjectMapper();
        StringEntity entity = new StringEntity(mapper.writeValueAsString(dataMap), "UTF-8");
        
        HttpPost httpPost = new HttpPost(url);
        entity.setContentType("application/json");
        httpPost.setEntity(entity);
        httpPost.addHeader("Accept", "application/json");
        
        return exec_request((HttpRequestBase) httpPost);
    }

    private Map<String, Object> PUT(int service_id, String uri, String slug, String filepath, String querystr, int max_bps, int chunk_size) throws HttpResponseException, IOException, ClassCastException
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

    @SuppressWarnings("unchecked")
    private Map<String, Object> exec_request(HttpRequestBase httpMethod) throws HttpResponseException, IOException, ClassCastException
    {
        DefaultHttpClient httpClient = new DefaultHttpClient();
        httpClient.getParams().setParameter("http.useragent", this.user_agent_name);

        BASE64Encoder enc = new sun.misc.BASE64Encoder();
        String userpassword = this.username + ":" + this.password;
        String encodedAuthorization = enc.encode( userpassword.getBytes() );
        httpMethod.addHeader("Authorization", "Basic "+ encodedAuthorization);

        HttpResponse response = httpClient.execute(httpMethod);
        if (response.getStatusLine().getStatusCode() > 299) {
            // try to get the reasons from the json error response
            String strErr = getReasonsFromErrorMsg(response);
            if (strErr.isEmpty()) {
                // if we can't get the reasons, dump the entire response body
                strErr = inputStreamToString(response.getEntity().getContent()).toString();
            }
            throw new HttpResponseException(response.getStatusLine().getStatusCode(), strErr);
        }
        ObjectMapper mapper2 = new ObjectMapper();
        Object responseObject = mapper2.readValue(response.getEntity().getContent(), Object.class);
        httpMethod.releaseConnection();
        return (Map<String, Object>) responseObject;
    }

    @SuppressWarnings("unchecked")
    private String getReasonsFromErrorMsg(HttpResponse errResponse)
    {
        String strReasons = "";
        try 
        {
            ObjectMapper mapper2 = new ObjectMapper();
            Map<String, Object> errBody = (Map<String, Object>) mapper2.readValue(errResponse.getEntity().getContent(), Object.class);
            List<String> reasonsObj = (List<String>) errBody.get("reasons");
            Iterator<String> iter = reasonsObj.iterator();
            while ( iter.hasNext() ) {
                if (! strReasons.isEmpty()) {
                    strReasons += ", ";
                }
                strReasons += iter.next();
            }
        }           
        catch (Exception ex)
        {
            LOGGER.log(Level.WARNING, "Error while parsing error response: {0}", ex);
        }
        return strReasons;
    }

    
    public Map<String, Object> rass_getDir(String cdn_path, String querystr) throws HttpResponseException, IOException, ClassCastException
    {
        String uri = "dir/" + lstrip(cdn_path, "/");
        
        return GET(RawsClient.RASS, uri, querystr);
    }
    
    public Map<String, Object> rass_putItem(String cdn_path, String cdn_filename, String filepath) throws HttpResponseException, IOException, ClassCastException
    {
        String uri = "item/" + lstrip(cdn_path, "/");
        
        return PUT(RawsClient.RASS, uri, cdn_filename, filepath, null, 0, 0);
    }

    public Map<String, Object> rass_putItem(String cdn_path, String cdn_filename, String filepath, int maxBps, int chunkSize) throws HttpResponseException, IOException, ClassCastException
    {
        String uri = "item/" + lstrip(cdn_path, "/");
        
        return PUT(RawsClient.RASS, uri, cdn_filename, filepath, null, maxBps, chunkSize);
    }
    
    
    public Map<String, Object> meta_postWslide(String webcastID, String path, String timestamp, String offset) throws HttpResponseException, IOException, ClassCastException
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
    
}
