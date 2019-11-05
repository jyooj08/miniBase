package minibase;

import java.util.*;


/**
 * @param <K> Type of the Cache Key
 * @param <V> Type of the Cache Elements
 */
public class LruCache <K, V> {

	HashMap<K,V> cache;
	HashMap<Integer,K> latency;
	int capacity, latencyNum, findNum;
	/**
	 * Constructor
	 * @param capacity
	 */
	public LruCache (int capacity) {
	  // TODO: some code goes here 
	  cache = new HashMap<K,V>();
	  latency = new HashMap<Integer,K>();
	  this.capacity = capacity; latencyNum=0; findNum=0;
	}

	public boolean isCached(K key) {
	  // TODO: some code goes here 
	  if(cache.containsKey(key)) return true;
	  else return false;
	}

	public V get (K key) {
	  // TODO: some code goes here 
	  if(isCached(key)) return cache.get(key);
	  else return null;
	}

	public void put (K key, V value) {
	  // TODO: some code goes here 
	  if(size() <= capacity){
	  	cache.put(key,value);
	  	latency.put(latencyNum++,key);
	  } 
	}

	public V evict() {
	  // TODO: some code goes here 
	  boolean find=false;
	  K key=null;
	  while(find){
	  	if(latency.containsKey(findNum)){
	  		key = latency.get(findNum);
	  		find=true;
	  		latency.remove(findNum);
	  		findNum++;
	  	}
	  }
	  V value = cache.get(key);
	  cache.remove(key);
	  return value;
	}

	public int size() {
	  // TODO: some code goes here 
	  return cache.size();
	}
	
	public Iterator<V> iterator() {
	  // TODO: some code goes here,,, please implement freely! you can remove and make new method 
	  Iterator<K> itr = cache.keySet().iterator();
	  HashSet<V> valueSet = new HashSet<V>();
	  
	  while(itr.hasNext()){
	  	K key = itr.next();
	  	valueSet.add(cache.get(key));
	  }
	  return valueSet.iterator();
	}


}
