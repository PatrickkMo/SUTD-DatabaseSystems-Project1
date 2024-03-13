package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc td;
    private RandomAccessFile raf;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) throws FileNotFoundException {
        file = f;
        this.td = td;
        raf = new RandomAccessFile(f, "rw");
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
       return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try {
            raf.seek(BufferPool.getPageSize() * pid.getPageNumber());
            byte[] buf = new byte[BufferPool.getPageSize()];
            raf.readFully(buf,0, BufferPool.getPageSize());
            return new HeapPage((HeapPageId) pid, buf);
        } catch (IOException e) {
            return null;
        }

    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        raf.seek(BufferPool.getPageSize() * page.getId().getPageNumber());
        byte[] buf = page.getPageData();
        raf.write(buf,0,buf.length);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        try {
            return (int) (raf.length()/ BufferPool.getPageSize());
        } catch (IOException e) {
            return 0;
        }
    }
    

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    private class HeapFileIterator implements DbFileIterator {
        int numPages = numPages();
        int currentPage = -1;
        Iterator<Tuple> pageIterator;
        private TransactionId tid;

        HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }



        @Override
        public void open() throws DbException, TransactionAbortedException {
        
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            while (currentPage < numPages) {
                if(pageIterator != null && pageIterator.hasNext()) {
                    return true;
                }
                pageIterator = null;
                currentPage++;
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(0, currentPage), Permissions.READ_WRITE);
                pageIterator = page.iterator();
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return pageIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            currentPage = 0;
            pageIterator = null;
        }

        @Override
        public void close() {
   
        }

    }

    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}

