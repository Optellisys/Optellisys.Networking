using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Subjects;
using System.Reactive.Linq;
using System.Configuration;

namespace Optellisys.Cluster
{

    /// <summary>
    /// Manages resources in a ring and handles calls for position on a ring
    /// </summary>
    /// <typeparam name="T">The type of object to be stored on the ring</typeparam>
    public class HashRing<T>
    {
        SortedDictionary<ulong, T> ring = new SortedDictionary<ulong, T>();
        ulong[] hashKeys = null;

        object ringLock = new object();

        public event EventHandler<int> OnRebalanced;
        public event EventHandler<T> OnNodeAdded;
        public event EventHandler<T> OnNodeRemoved;

        private System.Timers.Timer tHashRing;
        private int nodeReplicationFactor = 5;

        
        public HashRing(int _nodeReplicationFactor)
        {
            nodeReplicationFactor = _nodeReplicationFactor;
        }

        /// <summary>
        /// Starts the ring with a balanced set of objects.
        /// </summary>
        /// <param name="nodes">The objects to act as nodes around the ring.</param>
        public void Start(IEnumerable<T> nodes)
        {
            lock (ringLock)
            {
                foreach (T node in nodes)
                {
                    this.Add(node, false);
                }
                Rebalance();
            }
        }

        /// <summary>
        /// Adds a node to the ring and rebalances
        /// </summary>
        /// <param name="node">The object to add to the ring as a node.</param>
        public void Add(T node)
        {
            Add(node, true);
        }

        /// <summary>
        /// Adds a node to the ring with the option to rebalance
        /// </summary>
        /// <param name="node">The object to add to the ring as a node.</param>
        /// <param name="rebalance">Whether the nodes on the ring should be rebalanced.</param>
        private void Add(T node, bool rebalance)
        {
            if (!ring.Any(i => i.Value.ToString().Equals(node.ToString())))
            {
                lock (ringLock)
                {
                    for (int i = 0; i < nodeReplicationFactor; i++)
                    {
                        ulong hash = CalculateHash(node.GetHashCode().ToString() + i);
                        ring[hash] = node;
                    }
                    if (this.OnNodeAdded != null)
                    {
                        this.OnNodeAdded(this, node);
                    }
                    if (rebalance)
                    {
                        Rebalance();
                    }
                }

            }
        }

        /// <summary>
        /// Syncronizes the ring with the given list of nodes
        /// </summary>
        /// <param name="nodeList"></param>
        public void Sync(IEnumerable<T> nodeList)
        {
            bool changes = false;
            lock (ringLock)
            {
                foreach (T node in nodeList)
                {
                    if (!ring.Any(i => i.Value.ToString().Equals(node.ToString())))
                    {
                        changes = true;
                        for (int i = 0; i < nodeReplicationFactor; i++)
                        {
                            ulong hash = CalculateHash(node.GetHashCode().ToString() + i);
                            ring[hash] = node;
                        }
                        if (this.OnNodeAdded != null)
                        {
                            this.OnNodeAdded(this, node);
                        }
                    }
                }
                T[] distinctNodes = ring.Select(s => s.Value).Distinct().ToArray();
                foreach (T node in distinctNodes)
                {
                    if (!nodeList.Any(n => n.ToString().Equals(node.ToString())))
                    {
                        changes = true;
                        ulong[] hashes = ring.Where(n => n.Value.ToString().Equals(node.ToString())).Select(kv => kv.Key).ToArray();
                        foreach (var h in hashes)
                        {
                            ring.Remove(h);
                        }
                        if (this.OnNodeRemoved != null)
                        {
                            this.OnNodeRemoved(this, node);
                        }
                    }
                }

                //Swap and re-balance
                if (changes)
                {
                    Rebalance();
                }
            }
        }

        /// <summary>
        /// Removes a node from the ring
        /// </summary>
        /// <param name="node">The node to remove from the ring</param>
        public void Remove(T node)
        {
            lock (ringLock)
            {
                ulong[] hashes = ring.Where(n => n.Value.ToString().Equals(node.ToString())).Select(kv => kv.Key).ToArray();
                foreach (var h in hashes)
                {
                    ring.Remove(h);
                }
                if (this.OnNodeRemoved != null)
                {
                    this.OnNodeRemoved(this, node);
                }
                Rebalance();

            }
        }

        /// <summary>
        /// Finds the hash key that is greater than or equal to the specified value
        /// </summary>
        /// <param name="hashList">The list of hashe keys to search</param>
        /// <param name="hashValue">The value to find in the list</param>
        /// <returns></returns>
        private int FindHash(ulong[] hashList, ulong hashValue)
        {
            int start = 0;
            int end = hashList.Length - 1;

            if (hashList[end] < hashValue || hashList[0] > hashValue)
            {
                return 0;
            }

            int median = start;
            while (end - start > 1)
            {
                median = (end + start) / 2;
                if (hashList[median] >= hashValue)
                {
                    end = median;
                }
                else
                {
                    start = median;
                }
            }

            if (hashList[start] > hashValue || hashList[end] < hashValue)
            {
                throw new Exception("should not happen");
            }

            return end;
        }

        /// <summary>
        /// Hashes the given key and finds the element in the ring responsible for that key
        /// </summary>
        /// <param name="key">The key to search the ring for</param>
        /// <returns>The node responsible for that element</returns>
        public T GetNode(string key)
        {
            lock (ringLock)
            {
                ulong hash = CalculateHash(key);
                int first = FindHash(hashKeys, hash);
                return ring[hashKeys[first]];
            }
        }

        /// <summary>
        /// Rebalances the ring in an even configuration based on node count
        /// </summary>
        private void Rebalance()
        {
            SortedDictionary<ulong, T> balancedCircle = new SortedDictionary<ulong, T>();

            if (ring.Count > 1)
            {
                ulong gap = ulong.MaxValue / (ulong)ring.Count;
                ulong position = 0;
                foreach (var k in ring.Keys)
                {
                    balancedCircle.Add(position, ring[k]);
                    //Console.WriteLine("Rebalancing: {0} moved from {1} to {2} ({3})", circle[k], k, position, position > k ? position - k : k - position);

                    if (position > (ulong.MaxValue - gap))
                    {
                        position = ulong.MaxValue;
                    }
                    else
                    {
                        position += gap;
                    }
                }
            }
            else
            {
                balancedCircle.Add(0, ring.FirstOrDefault().Value);
            }
            ring = balancedCircle;
            hashKeys = ring.Keys.ToArray();

            if (this.OnRebalanced != null)
            {
                this.OnRebalanced(this, hashKeys.Length);
            }
        }

        /// <summary>
        /// Implementation of Knuth hash taked from http://stackoverflow.com/questions/9545619/a-fast-hash-function-for-string-in-c-sharp
        /// </summary>
        /// <param name="key">The string to hash</param>
        /// <returns>An unsigned long representation of the key</returns>
        static ulong CalculateHash(string key)
        {
            ulong hashedValue = 3074457345618258791ul;
            for (int i = 0; i < key.Length; i++)
            {
                hashedValue += key[i];
                hashedValue *= 3074457345618258799ul;
            }
            return hashedValue;
        }
    }
}
