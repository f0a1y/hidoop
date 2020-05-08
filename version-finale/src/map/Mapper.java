package map;

import java.io.Serializable;

import formats.FormatReader;
import formats.KV;
import ordo.SynchronizedList;

public interface Mapper extends Serializable {
	
	public void map(FormatReader reader, SynchronizedList<KV> channel);

}
