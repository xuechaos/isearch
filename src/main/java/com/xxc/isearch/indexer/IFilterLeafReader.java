package com.tudou.isearch.indexer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

import com.tudou.isearch.Model;

public class IFilterLeafReader<T extends Model> extends FilterLeafReader {
	
	private static final Logger logger = Logger.getLogger(IFilterLeafReader.class);
	private DeleteQueue<T> deleteQueue;
	private int cardinality;

	public IFilterLeafReader(LeafReader in, DeleteQueue<T> deleteQueue) {
		super(in);
		this.deleteQueue = deleteQueue;
	}

	@Override
	public Bits getLiveDocs() {
		ensureOpen();
		Iterator<String> it = deleteQueue.getQueue().iterator();
		FixedBitSet bits = (FixedBitSet) super.getLiveDocs();
		while (it.hasNext()) {
			PostingsEnum pe;
			try {
				pe = in.postings(new Term(deleteQueue.getUniqueKeyName(),
						it.next()));
				if (pe != null) {
					bits.set(pe.nextDoc());
					cardinality ++;
				}
			} catch (IOException e) {
				logger.error("!!!! IO Exception", e);
			}
		}
		return bits;
	}

	@Override
	public int numDocs() {
		return in.numDocs() - cardinality;
	}

}
