/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package rawsclient;

import java.io.*;
import org.apache.http.entity.AbstractHttpEntity;

/**
 * A streamed, non-repeatable entity that obtains its content from 
 * an {@link InputStream}.
 * 
 * @since 4.0
 */
public class ThrottledEntity extends AbstractHttpEntity {

    private final File file;
    private final InputStream content;
    private final long length;
    private boolean consumed = false;

    private final int bufferSize; // nr of bytes to be sent in a single operation, after which bps-check is made (e.g. 2048)
    private final long maxBytesPerSec;
    private final long sleepDurationMsecs; // nr of msecs to sleep if maxBytesPerSec is exceeded (e.g. 200)

    private long startTime;
    private long bytesSent;
    private long totalSleepTime;

    public ThrottledEntity(final File file, final String contentType, long max_bps, int buffer_size, long sleep_duration_msecs) throws FileNotFoundException
    {
        this.file = file;
        this.content = new FileInputStream(this.file);
        this.length = file.length();
        this.maxBytesPerSec = max_bps;
        this.bufferSize = buffer_size;
        this.sleepDurationMsecs = sleep_duration_msecs;
        
        setContentType(contentType);
    }

    public boolean isRepeatable() {
        return false;
    }

    public long getContentLength() {
        return this.length;
    }

    public InputStream getContent() throws IOException {
        return this.content;
    }
        
    public void writeTo(final OutputStream outstream) throws IOException 
    {
        if (outstream == null) {
            throw new IllegalArgumentException("Output stream may not be null");
        }
        
        startTime = System.currentTimeMillis();
        bytesSent = 0;
        
        InputStream instream = this.content;
        byte[] buffer = new byte[bufferSize];
        int l;
        if (this.length < 0) 
        {
            // consume until EOF
            while ((l = instream.read(buffer)) != -1) 
            {
                outstream.write(buffer, 0, l);
            }
        } 
        else 
        {
            // consume no more than length
            long remaining = this.length;
            while (remaining > 0) 
            {
                l = instream.read(buffer, 0, (int)Math.min(bufferSize, remaining));
                if (l == -1) {
                    break;
                }
                outstream.write(buffer, 0, l);
                bytesSent += l;
                remaining -= l;
                throttle();
            }
        }
        this.consumed = true;
    }

    // non-javadoc, see interface HttpEntity
    public boolean isStreaming() {
        return !this.consumed;
    }

    // non-javadoc, see interface HttpEntity
    public void consumeContent() throws IOException {
        this.consumed = true;
        // If the input stream is from a connection, closing it will read to
        // the end of the content. Otherwise, we don't care what it does.
        this.content.close();
    }
    
    private void throttle() throws IOException 
    {
        System.out.println("throttle() called");
        try 
        {
            while (getBytesPerSec() > maxBytesPerSec) 
            {
                Thread.sleep(sleepDurationMsecs);
                totalSleepTime += sleepDurationMsecs;
                System.out.println("throttle() : bps = " + getBytesPerSec() + " , total sleeptime " + totalSleepTime);
            }
        }
        catch (InterruptedException e) 
        {
            throw new IOException("Thread aborted", e);
        }
    }

    public long getBytesPerSec() 
    {
        long elapsed = (System.currentTimeMillis() - startTime) / 1000;
        if (elapsed == 0) {
            return bytesSent;
        } 
        else {
            return bytesSent / elapsed;
        }
    }

    public long getTotalBytesRead() {
        return bytesSent;
    }


    public long getTotalSleepTime() {
        return totalSleepTime;
    }


    
} // class InputStreamEntity
