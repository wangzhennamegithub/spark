/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.storage.memory

import java.util
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait MemoryEntryManager[K, V] {
  def getEntry(blockId: K): V

  def putEntry(key: K, value: V): V

  def removeEntry(key: K): V

  def clear()

  def containsEntry(key: K): Boolean
} //接口函数

class FIFOMemoryEntryManager[K, V] extends MemoryEntryManager[K, V] {
  val entries = new util.LinkedHashMap[K, V](32, 0.75f)

  override def getEntry(key: K): V = {
    entries.synchronized {
      entries.get(key)
    }
  }

  override def putEntry(key: K, value: V): V = {
    entries.synchronized {
      entries.put(key, value)
    }
  }

  def clear() {
    entries.synchronized {
      entries.clear()
    }
  }

  override def removeEntry(key: K): V = {
    entries.synchronized {
      entries.remove(key)
    }
  }

  override def containsEntry(key: K): Boolean = {
    entries.synchronized {
      entries.containsKey(key)
    }
  }
}  //FIFO

class LRUMemoryEntryManager[K, V] extends MemoryEntryManager[K, V] {
  def entrySet() : util.Set[util.Map.Entry[K, V]] = {
    entries.entrySet()
  }

  val entries = new util.LinkedHashMap[K, V](32, 0.75f, true)

  override def getEntry(key: K): V = {
    entries.synchronized {
      entries.get(key)
    }
  }

  override def putEntry(key: K, value: V): V = {
    entries.synchronized {
      entries.put(key, value)
    }
  }

  def clear() {
    entries.synchronized {
      entries.clear()
    }
  }

  override def removeEntry(key: K): V = {
    entries.synchronized {
      entries.remove(key)
    }
  }

  override def containsEntry(key: K): Boolean = {
    entries.synchronized {
      entries.containsKey(key)
    }
  }
}   //LRU


/*
class MCESCache[K, V] extends MemoryEntryManager[K, V]{

  val byKey = mutable.HashMap[K, MCESItem]()
  val frequencyHead = FrequencyNode(-1)

  def deleteNode(node: FrequencyNode): Unit = {
    node.prev.next = node.next
    node.next.prev = node.prev
    node.next= null; node.prev = null
  }

  def access(key: K): Int = {
    val tmp = this.byKey(key)
    if (tmp == null) {
      throw new Exception("No such key")
    }

    val freq = tmp.parent
    var nextFreq = freq.next

    if (nextFreq == this.frequencyHead || nextFreq.value != (freq.value + 1)) {
      nextFreq = MCESCache.getNewNode(freq, nextFreq, freq.value + 1)
    }

    nextFreq.items += this.byKey(key)
    tmp.parent = nextFreq

    nextFreq.prev.items -= nextFreq.prev.items.filter(freq => freq.key == key).head
    if (nextFreq.prev.items.isEmpty){
      this.deleteNode(freq)
    }

    tmp.data
  }

  def insert(key: K, value : Int) = {
    if (this.byKey.contains(key)){
      throw new Exception("Key already exists")
    }

    val freqNode = this.frequencyHead.next
    if (freqNode.value != 1){
      this.frequencyHead.next = MCESCache.getNewNode(this.frequencyHead, freqNode)
      this.byKey(key) = MCESItem(this.frequencyHead.next, value, key)
      this.frequencyHead.next.items += this.byKey(key)
    }else{
      this.byKey(key) = MCESItem(freqNode, value, key)
      freqNode.items += this.byKey(key)
    }

  }

  def getMCESItem(): MCESItem = {
    if (this.byKey.isEmpty) {
      throw new Exception("The set is empty")
    }
    this.byKey(this.frequencyHead.next.items.head.key)
  }
}   //MCES双链表
case class MCESItem(var parent: FrequencyNode, data : Int, key:= K.randomUUID()){ }

case class FrequencyNode(value: Int = 1) {
  val items: mutable.ArrayBuffer[MCESItem] = mutable.ArrayBuffer[MCESItem]()
  var prev: FrequencyNode = this
  var next: FrequencyNode = this
}

object MCESCache {

  def getNewNode(prev: FrequencyNode, next: FrequencyNode, freqValue: Int = 1): FrequencyNode = {
    val node = FrequencyNode(freqValue)
    node.prev = prev
    node.next = next
    node.prev.next = node
    node.next.prev = node
    node
  }

}
*/