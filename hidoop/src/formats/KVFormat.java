package formats;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class KVFormat implements Format {
    private static final long serialVersionUID = 1L;

    private String fname;

    private transient BufferedReader br;
    private transient BufferedWriter bw;
    private transient long index = 0;
    private transient Format.OpenMode mode;

    public KVFormat(String fname) {
        this.fname = fname;
    }

    public void open(OpenMode mode) {
        try {
            this.mode = mode;
            switch (mode) {
            case R:
                br = new BufferedReader(new InputStreamReader(new FileInputStream(fname)));
                break;
            case W:
                bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fname)));
                break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            switch (mode) {
            case R:
                br.close();
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
        	String cle = readTexte();
        	if (cle != null) {
        		String valeur = readTexte();
        		index += cle.length() + valeur.length();
        		return new KV(cle, valeur);
        	}
        	return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
    
    private String readTexte() throws IOException {
    	StringBuilder texte = new StringBuilder();
    	String ligne;
    	do {
    		ligne = br.readLine();
    		if (ligne != null) {
    			if (!ligne.endsWith(KV.SEPARATOR)) {
    				texte.append(ligne + '\n');
    			} else {
    				texte.append(ligne.subSequence(0, ligne.length() - KV.SEPARATOR.length()));
    			}
    		}
    	} while(ligne != null && !ligne.endsWith(KV.SEPARATOR));
        if (ligne != null) {
        	return texte.toString();
        } 
        return null;
    }

    public void write(KV record) {
        try {
        	String line = record.k + KV.SEPARATOR;
        	bw.write(line, 0, line.length());
            index += line.length();
            bw.newLine();
        	line = record.v + KV.SEPARATOR;
            index += line.length();
        	bw.write(line, 0, line.length());
            bw.newLine();
            bw.flush();
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
