package map;

import java.io.Serializable;

import formats.FormatWriter;
import formats.KV;
import ordo.SynchronizedList;

public interface Reducer extends Serializable {
	
	public void reduce(SynchronizedList<KV> channel, FormatWriter writer);

}
