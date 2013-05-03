// jTDS JDBC Driver for Microsoft SQL Server and Sybase
// Copyright (C) 2004 The jTDS Project
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

package net.sourceforge.jtds.jdbc.cache;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p> Simple LRU cache for any type of object, based on a {@link LinkedHashMap}
 * with a maximum size. </p>
 *
 * @author
 *    Holger Rehn
 */
public class SimpleLRUCache<K,V>
{

   // private instance fields //////////////////////////////////////////////////

   /**
    * map backing the LRU cache
    */
   private final Map<K,V> _Map;

   // public constructors //////////////////////////////////////////////////////

   /**
    * <P> Constructs a new LRU cache with a limited capacity. </p>
    *
    * @param limit
    *    maximum number of entries in this cache
    */
   public SimpleLRUCache( final int limit )
   {
      _Map = new LinkedHashMap<K,V>( limit + 10, 0.75f, true )
      {
         @Override
         protected boolean removeEldestEntry( Map.Entry<K,V> eldest )
         {
            return size() > limit;
         }
      };
   }

   // public methods ///////////////////////////////////////////////////////////

   /**
    * <p> Updates the LRU cache by adding a new entry. </p>
    *
    * @see
    *    java.util.Map#put(Object,Object)
    *
    * @param key
    *    key with which the specified value is to be associated
    *
    * @param value
    *    value to be associated with the specified key
    *
    * @return
    *    previous value associated with key or {@code null} if there was no
    *    mapping for key; a {@code null} value can also indicate that the cache
    *    previously associated {@code null} with the specified key
    */
   public synchronized V put( K key, V value )
   {
      return _Map.put( key, value );
   }

   /**
    * <p> Get the value associated with the given key, if any. </p>
    *
    * @see
    *    java.util.Map#get(Object)
    *
    * @param key
    *    the key whose associated value is to be returned
    *
    * @return
    *    the value to which the specified key is mapped, or {@code null} if this
    *    map contains no mapping for the key
    */
   public synchronized V get( K key )
   {
      return _Map.get( key );
   }

}