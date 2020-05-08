package formats;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;


public class LineFormat implements Format {
    private static final long serialVersionUID = 1L;

    private String fname;
    private KV kv;

    private transient LineNumberReader lnr;
    private transient BufferedWriter bw;
    private transient long index = 0;
    private transient Format.OpenMode mode;

    public LineFormat(String fname) {
        this.fname = fname;
    }

    public boolean open(Format.OpenMode mode) {
        boolean result = false;
    	try {
            this.mode = mode;
            this.kv = new KV();
            switch (mode) {
            case R:
                lnr = new LineNumberReader(new InputStreamReader(new FileInputStream(fname)));
                break;
            case W:
                bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fname)));
                break;
            }
            result = true;
        } catch (IOException e) {
            e.printStackTrace();
        }
    	return result;
    }

    public void close() {
        try {
            switch (mode) {
            case R:
                lnr.close();
                break;
            case W:
                bw.close();
                break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public KV read() {
        try {
        	StringBuilder texte = new StringBuilder();
        	String ligne;
        	boolean lignIncomplete = true;
        	do {
        		ligne = lnr.readLine();
        		if (ligne != null) {
        			if (!ligne.endsWith(KV.SEPARATOR)) {
        				texte.append(ligne + '\n');
        				lignIncomplete = false;
        			} else 
        				texte.append(ligne.subSequence(0, ligne.length() - KV.SEPARATOR.length()));
        		} else
        			lignIncomplete = false;
        	} while (lignIncomplete);
            if (texte.length() > 0) {
            	kv.k = Integer.toString(lnr.getLineNumber());
            	kv.v = texte.toString();
            	index += kv.v.length();
            	return kv;
            } 
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

	public KV[] read(byte[] buffer, int limit) {
		// TODO Auto-generated method stub
		return null;
	}

    public void write(KV record) {
        try {
            String ligne = record.v+KV.SEPARATOR;
            bw.write(ligne, 0, ligne.length());
            bw.newLine();
            index += record.v.length();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long getIndex() {
        return index;
    }

    public String getFname() {
        return fname;
    }

    public void setFname(String fname) {
        this.fname = fname;
    }
}