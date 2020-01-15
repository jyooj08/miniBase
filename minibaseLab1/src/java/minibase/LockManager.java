package minibase;

import java.util.*;
import java.io.*;

/**
 * LockManager is class which manages locks for transactions.
 * It stores states of various locks on pages and provides atomic grant
 * and release of locks.
 * @author hrishi
 */
public class LockManager {
    
    private HashMap<PageId, Set<TransactionId>> readLocks;
    private HashMap<PageId, TransactionId> writeLock;
    private HashMap<TransactionId, Set<PageId>> sharedPages;
    private HashMap<TransactionId, Set<PageId>> exclusivePages;

    public LockManager() {
    	readLocks = new HashMap<PageId, Set<TransactionId>>();
        writeLock = new HashMap<PageId, TransactionId>();
        sharedPages = new HashMap<TransactionId, Set<PageId>>();
        exclusivePages = new HashMap<TransactionId, Set<PageId>>();
    }
    
    private void addLock(TransactionId tid, PageId pid, Permissions perm){
    	if(perm.equals(Permissions.READ_ONLY)){
    		if(!readLocks.containsKey(pid)) readLocks.put(pid, new HashSet<TransactionId>());
    		readLocks.get(pid).add(tid);
    		
    		if(!sharedPages.containsKey(tid)) sharedPages.put(tid, new HashSet<PageId>());
    		sharedPages.get(tid).add(pid);
    	} else{
    		writeLock.put(pid,tid);
    		if(!exclusivePages.containsKey(tid)) exclusivePages.put(tid, new HashSet<PageId>());
    		exclusivePages.get(tid).add(pid);
    	}
    }
    
    /**
     * Checks if transaction has lock on a page
     * @param tid Transaction Id
     * @param pid Page Id
     * @return boolean True if holds lock
     */
    public boolean holdsLock(TransactionId tid, PageId pid){
        if(readLocks.containsKey(pid)){
        	Set set = readLocks.get(pid);
        	if(set.contains(tid)) return true;
        }
        
        if(writeLock.containsKey(pid)){
        	TransactionId t = writeLock.get(pid);
        	if(t.equals(tid)) return true;
        }
        return false;
    }
    
    private boolean grantLock(TransactionId tid, PageId pid, Permissions perm){
    	if(perm.equals(Permissions.READ_ONLY)){
    		if(writeLock.containsKey(pid) && !writeLock.get(pid).equals(tid)) return false;
    		addLock(tid,pid,perm);
    		return true;
    	}
    	
    	if(!writeLock.containsKey(pid) && (!readLocks.containsKey(pid) || readLocks.get(pid).isEmpty())){
    		addLock(tid,pid,perm);
    		return true;
    	}
    	
    	if(readLocks.containsKey(pid) && readLocks.get(pid).contains(tid) && readLocks.get(pid).size()==1){
    		addLock(tid, pid, perm);
    		return true;
    	}
    	
    	if(exclusivePages.containsKey(tid) && exclusivePages.get(tid).contains(pid)) return true;
    	
    	return false;
    }
    
    /**
     * Grants lock to the Transaction.
     * @param tid TransactionId requesting lock.
     * @param pid PageId on which the lock is requested.
     * @param perm The type of permission.
     */
    public void requestLock(TransactionId tid, PageId pid, 
            Permissions perm) throws TransactionAbortedException{
    	int delay = 100, max_try = 200;
    	
    	if(sharedPages.containsKey(tid) || exclusivePages.containsKey(tid)){
    		delay = 10; max_try = 400;
    	}
    	
    	boolean isGranted = grantLock(tid,pid,perm);
    	long start = System.currentTimeMillis();
    	while(!isGranted){
    		if(System.currentTimeMillis() - start > max_try){
    			if(perm.equals(Permissions.READ_ONLY)) {
    				try{
    					Thread.sleep(delay);
    				} catch(Exception e){}
    				isGranted = grantLock(tid,pid,perm);
    			}
    			else{
    				isGranted = grantLock(tid,pid,perm);
    				start = System.currentTimeMillis();
    			}
    		}
    		
    	}    	
    }
    
    /**
     * Releases locks associated with given transaction and page.
     * @param tid The TransactionId.
     * @param pid The PageId.
     */
    public synchronized void releaseLock(TransactionId tid, PageId pid){
    	if(readLocks.containsKey(pid))
            readLocks.get(pid).remove(tid);
        if(writeLock.containsKey(pid))
        	writeLock.remove(pid);
        if(sharedPages.containsKey(tid))
            sharedPages.get(tid).remove(pid);
        if(exclusivePages.containsKey(tid))
            exclusivePages.get(tid).remove(pid);
    }
    
    /**
     * Releases Lock related to a page
     * @param pid PageId
     */
    public synchronized void removePage(PageId pid){
    	readLocks.remove(pid);
    	writeLock.remove(pid);
    }
    
    /**
     * Releases all pages associated with given Transaction.
     * @param tid The TransactionId.
     */
    public void releaseAllPages(TransactionId tid){
    	if(sharedPages.containsKey(tid)){
    		Set set = sharedPages.get(tid);
    		Iterator itr = set.iterator();
    		PageId pid = null;
    		while(itr.hasNext()){
    			pid = (PageId)itr.next();
    			readLocks.get(pid).remove(tid);
    		}
            sharedPages.remove(tid);
        }
        if(exclusivePages.containsKey(tid)){
        	Set set = exclusivePages.get(tid);
        	Iterator itr = set.iterator();
        	PageId pid = null;
        	while(itr.hasNext()){
        		pid = (PageId)itr.next();
        		writeLock.remove(pid);
        	}
            exclusivePages.remove(tid);
        }
    }
    
}
