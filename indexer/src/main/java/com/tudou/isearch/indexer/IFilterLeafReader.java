package com.tudou.isearch.indexer;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;


public class IFilterLeafReader extends FilterLeafReader {
	
	private static final Logger logger = Logger.getLogger(IFilterLeafReader.class);
	private DeleteQueue deleteQueue;
	private int cardinality;

	public IFilterLeafReader(LeafReader in, DeleteQueue deleteQueue) {
		super(in);
		this.deleteQueue = deleteQueue;
	}

	@Override
	public Bits getLiveDocs() {
		ensureOpen();
		LinkedBlockingQueue<String> delQueue = deleteQueue.getQueue();
		Iterator<String> it = delQueue.iterator();
		FixedBitSet bits = (FixedBitSet) super.getLiveDocs();
		logger.info(">>>>>>>>>>>>>>>deleteQueue="+delQueue+" count="+delQueue.size());
		logger.info(">>>>>>>>>>>>>>>getLiveDocs= "+bits);
		if(bits == null){
			bits = new FixedBitSet(this.maxDoc());
		}
		//while (it.hasNext()) {
			PostingsEnum pe;
			try {
				pe = in.postings(new Term("simpleId","123456"));
						//it.next()));
				if (pe != null && bits != null) {
					bits.set(pe.nextDoc());
					cardinality ++;
				}
			} catch (IOException e) {
				logger.error("!!!! IO Exception", e);
			}
		//}
		return bits;
	}

	@Override
	public int numDocs() {
		return in.numDocs() - cardinality;
	}

}
