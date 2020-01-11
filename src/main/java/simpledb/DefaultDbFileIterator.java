package simpledb;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class DefaultDbFileIterator implements DbFileIterator {

    private HeapFile dbFile;
    private TransactionId tid;
    private int pageCursor = 0;
    private Iterator<Tuple> tupleIterable;

    public DefaultDbFileIterator(HeapFile dbFile, TransactionId tid) {
        this.dbFile = dbFile;
        this.tid = tid;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        try {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(dbFile.getId(), pageCursor), Permissions.READ_ONLY);
            tupleIterable = page.iterator();
            Debug.log("Database.getBufferPool().getPage");
        } catch (TransactionAbortedException | DbException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (tupleIterable == null) {
            return false;
        }
        if (!tupleIterable.hasNext() && pageCursor >= dbFile.numPages() - 1) {
            return false;
        }
        if (!tupleIterable.hasNext()) {
            pageCursor++;
            tupleIterable = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(dbFile.getId(), pageCursor), Permissions.READ_ONLY)).iterator();
        }
//        System.out.println("pageCursor:"+pageCursor);
        return tupleIterable.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (tupleIterable == null || !hasNext()) {
            throw new NoSuchElementException();
        }
        return tupleIterable.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        open();
    }

    @Override
    public void close() {
        try {
            Database.getBufferPool().transactionComplete(tid);
            pageCursor = 0;
            tupleIterable = null;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
