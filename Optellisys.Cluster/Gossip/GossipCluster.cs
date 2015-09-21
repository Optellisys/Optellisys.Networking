using ProtoBuf;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using ZeroMQ;

namespace Optellisys.Cluster.Gossip {

    public class GossipCluster : IDisposable {
        protected GossipRegistry registry;

        private GossipNode localNode;
        private object registryLock = new object();

        private System.Timers.Timer t;

        private ZContext gossipListenerContext;
        private ZSocket gossipListenerSocket;

        private BlockingCollection<byte[]> messageQueue = new BlockingCollection<byte[]>();

        private int gossipJoinTimeout = 10000;

        private Task listener;
        private CancellationTokenSource listenerCancellation = new CancellationTokenSource();

        private Task queueProcessor;
        private CancellationTokenSource queueCancellation = new CancellationTokenSource();

        private List<System.Net.IPEndPoint> listenerAddressList = new List<IPEndPoint>();
        private List<System.Net.IPEndPoint> seedAddressList = new List<IPEndPoint>();
        private string clusterKey;
        private string partitionName;
        private string roles;

        private int localPartitionSuspectAfter = 20; //heartbeats
        private int localPartitionTombstoneAfter = 2000; //heartbeats
        private int remotePartitionSuspectAfter = 30; //heartbeats
        private int remotePartitionTombstoneAfter = 3000; //heartbeats
        private int gossipInterval = 200; //ms
        private int localPartitionGossipWeight = 5; //10x preference toward sending messages to other nodes within the partition

        /// <summary>
        /// Exposes the current registry for use by calling class
        /// </summary>
        public GossipRegistry Registry {
            get {
                return registry;
            }
        }

        public string PartitionName { get { return partitionName; } }

        public event EventHandler<GossipNode> OnNodeAdded;

        public event EventHandler<GossipNode> OnNodeSuspect;

        public event EventHandler<GossipNode> OnNodeSuspended;

        public event EventHandler<GossipNode> OnNodeTombstoned;

        /// <summary>
        /// Constructor that bootstraps address/port from the config
        /// </summary>
        public GossipCluster() {
            if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["Cluster.Gossip.Listener.Address"])) {
                listenerAddressList = Utilities.EndpointParser.ParseEndpoints(ConfigurationManager.AppSettings["Cluster.Gossip.Listener.Address"]);
            } else {
                //TODO: Log Error
            }

            if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["Cluster.Gossip.Seed.AddressList"])) {
                seedAddressList = Utilities.EndpointParser.ParseEndpoints(ConfigurationManager.AppSettings["Cluster.Gossip.Seed.AddressList"]);
            }

            if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["Cluster.Gossip.Interval"])) {
                if (!int.TryParse(ConfigurationManager.AppSettings["Cluster.Gossip.Interval"], out gossipInterval)) {
                    //TODO: Log Error
                }
            }
            if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["Cluster.Gossip.ClusterKey"])) {
                clusterKey = ConfigurationManager.AppSettings["Cluster.Gossip.ClusterKey"];
            }
            if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["Cluster.Gossip.LocalPartition.Name"])) {
                partitionName = ConfigurationManager.AppSettings["Cluster.Gossip.LocalPartition.Name"];
            }
            if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["Cluster.Roles"])) {
                roles = ConfigurationManager.AppSettings["Cluster.Roles"];
            }

            if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["Cluster.Gossip.JoinTimeout"])) {
                if (!int.TryParse(ConfigurationManager.AppSettings["Cluster.Gossip.JoinTimeout"], out gossipJoinTimeout)) {
                    //TODO: Log Error
                }
            }
            if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["Cluster.Gossip.LocalPartition.SuspectAfter"])) {
                if (!int.TryParse(ConfigurationManager.AppSettings["Cluster.Gossip.LocalPartition.SuspectAfter"], out localPartitionSuspectAfter)) {
                    //TODO: Log Error
                }
            }
            if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["Cluster.Gossip.LocalPartition.TombstoneAfter"])) {
                if (!int.TryParse(ConfigurationManager.AppSettings["Cluster.Gossip.LocalPartition.TombstoneAfter"], out localPartitionTombstoneAfter)) {
                    //TODO: Log Error
                }
            }
            if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["Cluster.Gossip.RemotePartition.SuspectAfter"])) {
                if (!int.TryParse(ConfigurationManager.AppSettings["Cluster.Gossip.RemotePartition.SuspectAfter"], out remotePartitionSuspectAfter)) {
                    //TODO: Log Error
                }
            }
            if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["Cluster.Gossip.RemotePartition.TombstoneAfter"])) {
                if (!int.TryParse(ConfigurationManager.AppSettings["Cluster.Gossip.RemotePartition.TombstoneAfter"], out remotePartitionTombstoneAfter)) {
                    //TODO: Log Error
                }
            }
            if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["Cluster.Gossip.LocalPartition.GossipWeight"])) {
                if (!int.TryParse(ConfigurationManager.AppSettings["Cluster.Gossip.LocalPartition.GossipWeight"], out localPartitionGossipWeight)) {
                    //TODO: Log Error
                }
            }
            Init();
        }

        /// <summary>
        /// Constructor that accepts address/port from caller
        /// </summary>
        /// <param name="listenerAddress">The IP address for the listener to bind to (* for all)</param>
        /// <param name="listenerPort">The port for the listener to bind to</param>
        public GossipCluster(List<System.Net.IPEndPoint> _listenerAddressList, int _gossipInterval, string _clusterKey, string _partitionName, string _roles, List<System.Net.IPEndPoint> _seedAddresses, int _joinTimeout, int _localPartitionSuspectAfter, int _localPartitionTombstoneAfter, int _remotePartitionSuspectAfter, int _remotePartitionTombstoneAfter, int _localPartitionGossipWeight) {
            //Build local node
            listenerAddressList = _listenerAddressList;
            seedAddressList = _seedAddresses;
            clusterKey = _clusterKey;
            partitionName = _partitionName;
            roles = _roles;
            gossipInterval = _gossipInterval;
            gossipJoinTimeout = _joinTimeout;
            localPartitionSuspectAfter = _localPartitionSuspectAfter;
            localPartitionTombstoneAfter = _localPartitionTombstoneAfter;
            remotePartitionSuspectAfter = _remotePartitionSuspectAfter;
            remotePartitionTombstoneAfter = _remotePartitionTombstoneAfter;
            localPartitionGossipWeight = _localPartitionGossipWeight;

            Init();
        }

        private void Init() {
            List<string> endpointList = new List<string>();
            foreach (IPEndPoint e in listenerAddressList) {
                endpointList.Add(string.Format("tcp://{0}:{1}", e.Address.ToString(), e.Port.ToString()));
            }

            localNode = new GossipNode() {
                ClusterKey = clusterKey,
                GossipHeartbeats = 0,
                ListenerAddressList = endpointList.ToArray(),
                NodeId = Guid.NewGuid(),
                HostName = Dns.GetHostName(),
                Partition = partitionName,
                Status = GossipStatus.Active,
                Suspect = false,
                SuspectMatrix = new Dictionary<Guid, bool>(),
                UpdateVersion = DateTime.Now.Ticks
            };
            //Set node roles
            List<string> nodeRoles = new List<string>();
            if (!string.IsNullOrEmpty(roles)) {
                string[] arrRoles = roles.Split(',');
                foreach (string r in arrRoles) {
                    if (!string.IsNullOrEmpty(r)) {
                        nodeRoles.Add(r.Trim());
                    }
                }
            }
            localNode.ClusterRoles = nodeRoles.ToArray();
        }

        /// <summary>
        /// Starts the cluster
        /// </summary>
        public void Start() {
            //Configure listener socket
            gossipListenerContext = new ZContext();
            gossipListenerSocket = new ZSocket(gossipListenerContext, ZSocketType.ROUTER);
            gossipListenerSocket.IdentityString = localNode.NodeId.ToString();
            foreach (string e in localNode.ListenerAddressList) {
                gossipListenerSocket.Bind(e);
            }

            //Configure listener thread
            listener = new Task(() => {
                var poll = ZPollItem.CreateReceiver();
                ZMessage incomming;
                ZError error;
                while (true) {
                    try {
                        // Receive
                        if (gossipListenerSocket.PollIn(poll, out incomming, out error, TimeSpan.FromMilliseconds(64))) {
                            using (incomming) {
                                messageQueue.Add(incomming[1].Read());

                                Debug.WriteLine("Received {0}", incomming[1].ReadString());
                            }
                        }
                    } catch (Exception ex) {
                        //TODO: Log errors
                    }
                    if (listenerCancellation.Token.IsCancellationRequested) {
                        //listenerSource.Token.ThrowIfCancellationRequested();
                        break;
                    }
                }
            }, listenerCancellation.Token);

            queueProcessor = new Task(() => {
                while (true) {
                    try {
                        byte[] m = messageQueue.Take(queueCancellation.Token);
                        //First 4 bytes indicates type of message
                        byte[] buffer = new byte[4];
                        Array.Copy(m, buffer, 4);
                        GossipMessageType msgType = (GossipMessageType)BitConverter.ToInt32(buffer, 0);

                        //Read the rest of the message into a stream
                        using (MemoryStream ms = new MemoryStream(m, 4, m.Length - 4)) {
                            //Process message by type
                            switch (msgType) {
                                case GossipMessageType.Heartbeat:
                                    try {
                                        //Deserialize message
                                        GossipRegistry reg = Serializer.Deserialize<GossipRegistry>(ms);

                                        //Verify cluster key
                                        if (reg.ClusterKey.Equals(localNode.ClusterKey)) {
                                            ProcessHearbeat(reg);
                                        } else {
                                            //TODO: Log Bad Key
                                        }
                                    } catch (Exception ex) {
                                        //TODO: Log Error
                                    }
                                    break;

                                case GossipMessageType.Join:
                                    try {
                                        //Deserialize message
                                        GossipNode joinUpdate = Serializer.Deserialize<GossipNode>(ms);

                                        //Verify cluster key
                                        if (joinUpdate.ClusterKey.Equals(localNode.ClusterKey)) {
                                            ProcessJoin(joinUpdate);
                                        } else {
                                            //TODO: Log Bad Key
                                        }
                                    } catch (Exception ex) {
                                        //TODO: Log Error
                                    }
                                    break;

                                case GossipMessageType.Leave:
                                    try {
                                        //Deserialize message
                                        GossipNode leaveUpdate = Serializer.Deserialize<GossipNode>(ms);

                                        //Verify cluster key
                                        if (leaveUpdate.ClusterKey.Equals(localNode.ClusterKey)) {
                                            ProcessLeave(leaveUpdate);
                                        } else {
                                            //TODO: Log Bad Key
                                        }
                                    } catch (Exception ex) {
                                        //TODO: Log Error
                                    }
                                    break;
                            }
                        }
                    } catch (Exception ex) {
                        //TODO: Handle exception
                    }
                    if (queueCancellation.IsCancellationRequested) {
                        break;
                    }
                }
            }, queueCancellation.Token);

            //Start processor
            queueProcessor.Start();

            //Start listener
            listener.Start();

            //Check for seed node
            bool joinedCluster = false;

            foreach (var seedHost in seedAddressList) {
                //Attempt to join cluster through seed node
                string gossipSeedAddress = string.Format("tcp://{0}:{1}", seedHost.Address.ToString(), seedHost.Port);
                Console.WriteLine("Contacting seed node {0}", gossipSeedAddress);

                registry = new GossipRegistry() {
                    ClusterKey = localNode.ClusterKey,
                    GossipList = new List<GossipNode>()
                };

                // Join cluster (cluster will add node to registry and return it back)
                DateTime joinRequestDate = DateTime.Now;
                List<byte> serializedJoinRequest = new List<byte>();
                using (MemoryStream ms = new MemoryStream()) {
                    //Serialize local node
                    Serializer.Serialize(ms, localNode);

                    ms.Position = 0;
                    byte[] serialBuffer = new byte[ms.Length];
                    ms.Read(serialBuffer, 0, serialBuffer.Length);

                    serializedJoinRequest.AddRange(BitConverter.GetBytes((int)GossipMessageType.Join));
                    serializedJoinRequest.AddRange(serialBuffer);

                    //Send local node to seed
                    using (var joinContext = new ZContext())
                    using (var joinSocket = new ZSocket(joinContext, ZSocketType.DEALER)) {
                        joinSocket.Linger = System.TimeSpan.FromMilliseconds(50);

                        // Bind
                        joinSocket.Connect(gossipSeedAddress);
                        //hbSocket.IdentityString = gossipListenerAddress;
                        joinSocket.Send(new ZFrame(serializedJoinRequest.ToArray()));

                        var joinReceiver = ZPollItem.CreateReceiver();
                        ZMessage joinMessage;
                        ZError joinError;
                        //joinSocket.PollIn(joinReceiver, out joinMessage, out joinError, TimeSpan.FromMilliseconds(100));
                        System.Threading.Thread.Sleep(1);
                    }

                    // While we haven't timed out
                    while (DateTime.Now.Subtract(joinRequestDate).TotalMilliseconds < gossipJoinTimeout) {
                        //Sleep 1 second and check registry to see if we heard back and we're added.
                        System.Threading.Thread.Sleep(1000);
                        if (registry.GossipList.Any(n => n.NodeId.Equals(localNode.NodeId))) {
                            break;
                        }
                    }
                }

                if (registry.GossipList.Any(n => n.NodeId.Equals(localNode.NodeId))) {
                    // We heard back from the cluster and we're a member!
                    joinedCluster = true;

                    Console.WriteLine("Successfully joined cluster.  Starting Gossip.");
                    break;
                } else {
                    //Join timeout
                    joinedCluster = false;
                    Console.WriteLine("Timeout joining cluster.  Exiting.");
                }
            }

            //If we didn't join the cluster through the seed node, start a new cluster
            if (!joinedCluster) {
                Console.WriteLine("Starting new cluster.");
                //  Start new cluster
                registry = new GossipRegistry() {
                    ClusterKey = localNode.ClusterKey,
                    GossipList = new List<GossipNode>()
                };
                registry.GossipList.Add(localNode);
            }

            //Start gossip timer
            t = new System.Timers.Timer(gossipInterval);
            t.Elapsed += GossipInterval_Elapsed;
            t.Enabled = true;
            t.Start();
        }

        /// <summary>
        /// Processes a heartbeat message
        /// </summary>
        /// <param name="reg">The registry received from the remote node</param>
        private void ProcessHearbeat(GossipRegistry reg) {
            try {
                // Merge received registry with local

                lock (registryLock) {
                    foreach (var node in reg.GossipList) {
                        //If remote node has node suspended, do not add or update it (stale data)
                        if (node.Status != GossipStatus.Suspended) {
                            // Get matching node from registry
                            var localRegNode = registry.GossipList.FirstOrDefault(n => n.NodeId.Equals(node.NodeId));
                            if (localRegNode == null) {
                                // Add the node to the local registry
                                registry.GossipList.Add(node);
                                if (this.OnNodeAdded != null) {
                                    this.OnNodeAdded(this, node);
                                }
                                localRegNode = registry.GossipList.FirstOrDefault(n => n.NodeId.Equals(node.NodeId));
                            }

                            // Update heartbeats to lowest values
                            localRegNode.GossipHeartbeats = Math.Min(localRegNode.GossipHeartbeats, node.GossipHeartbeats);
                            localRegNode.SuspectMatrix = node.SuspectMatrix;
                            if (!localRegNode.SuspectMatrix.ContainsKey(localNode.NodeId)) { localRegNode.SuspectMatrix.Add(localNode.NodeId, false); }

                            // Apply updates if version is newer
                            if (node.UpdateVersion > localRegNode.UpdateVersion) {
                                //Update node

                                if (localRegNode.ListenerAddressList != node.ListenerAddressList) { localRegNode.ListenerAddressList = node.ListenerAddressList; }
                                if (localRegNode.Partition != node.Partition) { localRegNode.Partition = node.Partition; }
                                if (localRegNode.HostName != node.HostName) { localRegNode.HostName = node.HostName; }
                                //Do not update status.  Every node must determine status on it's own.
                                //if (localRegNode.Status != node.Status) { localRegNode.Status = node.Status; }
                                localRegNode.UpdateVersion = node.UpdateVersion;
                            }
                        }
                    }
                }
            } catch (Exception ex) {
                //TODO: Log Error
            }
        }

        /// <summary>
        /// Processes a join request from another node
        /// </summary>
        /// <param name="joinUpdate">The details of the node requesting to join the cluster</param>
        private void ProcessJoin(GossipNode joinUpdate) {
            try {
                //Verify node has valid ID
                if (joinUpdate.NodeId == null) {
                    //TODO: Log Error
                    return;
                }
                //Verify it's not already in the registry
                GossipNode existingNode = registry.GossipList.FirstOrDefault(n => n.NodeId.Equals(joinUpdate.NodeId));
                if (existingNode != null) {
                    if (existingNode.Status != GossipStatus.Unknown &&
                        existingNode.Status != GossipStatus.Suspended) {
                        //TODO: Log Error
                        return;
                    }
                }
                if (string.IsNullOrEmpty(joinUpdate.Partition)) {
                    //TODO: Log Error
                    return;
                }
                joinUpdate.Status = GossipStatus.Active;
                joinUpdate.GossipHeartbeats = 0;
                joinUpdate.SuspectMatrix = new Dictionary<Guid, bool>();
                joinUpdate.UpdateVersion = DateTime.Now.Ticks;
                joinUpdate.Suspect = false;
                lock (registryLock) {
                    registry.GossipList.Add(joinUpdate);
                }
                if (this.OnNodeAdded != null) {
                    this.OnNodeAdded(this, joinUpdate);
                }
            } catch (Exception ex) {
                //TODO: Log Error
            }
        }

        /// <summary>
        /// Processes a leave request from another node
        /// </summary>
        /// <param name="leaveUpdate">The details of the node requesting to leave the cluster</param>
        private void ProcessLeave(GossipNode leaveUpdate) {
            // Update status of node to "Suspended"
            lock (registryLock) {
                var node = registry.GossipList.FirstOrDefault(n => n.NodeId.Equals(leaveUpdate.NodeId));
                if (node == null) {
                    //TODO: Log Error
                    return;
                }
                node.Status = GossipStatus.Suspended;
            }
            if (this.OnNodeSuspended != null) {
                this.OnNodeSuspended(this, leaveUpdate);
            }
        }

        /// <summary>
        /// Increments heartbeat info, local suspect info and the suspect matrix in the
        /// registry, then selects a random node to send it's registry to.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void GossipInterval_Elapsed(object sender, System.Timers.ElapsedEventArgs e) {
            t.Stop();
            t.Enabled = false;

            try {
                List<GossipNode> activeNodeList = new List<GossipNode>(registry.GossipList.Where(n => (n.Status == GossipStatus.Active || n.Status == GossipStatus.Suspect) && !n.NodeId.Equals(localNode.NodeId)));
                if (activeNodeList.Count > 0) // Other nodes in the cluster
                {
                    List<byte> serializedRegistry = new List<byte>();
                    using (MemoryStream ms = new MemoryStream()) {
                        //Lock the registry
                        lock (registryLock) {
                            //Loop through gossip list
                            foreach (var node in registry.GossipList) {
                                //If local node, set heartbeat interval to 0
                                if (node.NodeId.Equals(localNode.NodeId)) {
                                    node.GossipHeartbeats = 0;
                                } else //Otherwise increment heartbeat
                                  {
                                    node.GossipHeartbeats++;
                                }

                                //Add local node to suspect matrix if it doesn't exist
                                if (!node.SuspectMatrix.ContainsKey(localNode.NodeId)) { node.SuspectMatrix.Add(localNode.NodeId, false); }

                                int suspectAfter = node.Partition.Equals(localNode.Partition, StringComparison.InvariantCultureIgnoreCase) ? localPartitionSuspectAfter : remotePartitionSuspectAfter;
                                //If node is suspect, update suspect list and matrix
                                if (node.GossipHeartbeats > suspectAfter) {
                                    if (!node.Suspect) {
                                        node.Suspect = true;
                                        node.SuspectMatrix[localNode.NodeId] = true;
                                        if (this.OnNodeSuspect != null) {
                                            OnNodeSuspect(this, node);
                                        }
                                    }
                                } else {
                                    node.Suspect = false;
                                    node.SuspectMatrix[localNode.NodeId] = false;
                                }
                            }

                            // Check for active nodes suspected by consensus of other nodes that are not suspect themselves
                            //      Get active nodes that are not suspect
                            List<Guid> activeNonSuspect = new List<Guid>(registry.GossipList.Where(n => n.Status == GossipStatus.Active && n.Suspect == false).Select(n => n.NodeId));
                            foreach (var node in registry.GossipList.Where(n => n.Status == GossipStatus.Active || n.Status == GossipStatus.Suspect)) {
                                //Clean up suspect matrix (can't be suspected by a non-active node)
                                Dictionary<Guid, bool> newMatrix = new Dictionary<Guid, bool>(node.SuspectMatrix.Where(i => activeNonSuspect.Contains(i.Key)).ToDictionary(k => k.Key, v => v.Value));
                                node.SuspectMatrix = newMatrix;

                                //Concensus = at least half of activeNonSuspect nodes
                                if (node.SuspectMatrix.Count(s => s.Value == true && activeNonSuspect.Contains(s.Key)) >= Math.Round(((decimal)activeNonSuspect.Count) / 2, 0, MidpointRounding.AwayFromZero)) {
                                    node.Status = GossipStatus.Suspended;
                                    node.UpdateVersion = DateTime.Now.Ticks;
                                    if (this.OnNodeSuspended != null) {
                                        this.OnNodeSuspended(this, node);
                                    }
                                }
                            }

                            //Check suspended nodes to determine if they should be tombstoned (removed from registry)
                            foreach (var node in registry.GossipList.Where(n => n.Status == GossipStatus.Suspended)) {
                                int tombstoneAfter = node.Partition.Equals(localNode.Partition, StringComparison.InvariantCultureIgnoreCase) ? localPartitionTombstoneAfter : remotePartitionTombstoneAfter;
                                if (node.GossipHeartbeats > tombstoneAfter) {
                                    registry.GossipList.Remove(node);
                                    if (this.OnNodeTombstoned != null) {
                                        this.OnNodeTombstoned(this, node);
                                    }
                                }
                            }

                            //Serialize registry
                            Serializer.Serialize(ms, registry);
                        }

                        ms.Position = 0;
                        byte[] serialBuffer = new byte[ms.Length];
                        ms.Read(serialBuffer, 0, serialBuffer.Length);

                        serializedRegistry.AddRange(BitConverter.GetBytes((int)GossipMessageType.Heartbeat));
                        serializedRegistry.AddRange(serialBuffer);
                    }

                    //Select random host to connect to (Do not send to self or non-active nodes)
                    activeNodeList = new List<GossipNode>(registry.GossipList.Where(n => (n.Status == GossipStatus.Active || n.Status == GossipStatus.Suspect) && !n.NodeId.Equals(localNode.NodeId)));
                    if (activeNodeList.Count > 0) {
                        SortedDictionary<int, GossipNode> weightedNodes = new SortedDictionary<int, GossipNode>();
                        //Get count of nodes in/out of partition
                        int foreign = activeNodeList.Count(n => !n.Partition.Equals(localNode.Partition, StringComparison.InvariantCultureIgnoreCase));
                        int local = (activeNodeList.Count - foreign);

                        int totalWeight = (foreign + local) * localPartitionGossipWeight;
                        int localPartitionWeight = totalWeight - foreign;
                        int localNodeWeight = local == 0 ? 1 : localPartitionWeight / local; //Handle if node is alone in partition (and divide by zero)

                        //Apply weights
                        int nodeWeight = 0;
                        foreach (var n in activeNodeList) {
                            if (n.NodeId.Equals(localNode.NodeId)) {
                                //do nothing (we don't send to ourself)
                            } else if (n.Partition.Equals(localNode.Partition, StringComparison.InvariantCultureIgnoreCase)) {
                                nodeWeight += localNodeWeight;
                                weightedNodes.Add(nodeWeight, n);
                            } else {
                                nodeWeight += 1;
                                weightedNodes.Add(nodeWeight, n);
                            }
                        }

                        //Selected weighted random node
                        System.Random rnd = new Random();
                        int random = rnd.Next(0, nodeWeight);
                        GossipNode selectedNode = null;
                        foreach (int j in weightedNodes.Keys) {
                            if (j > random) {
                                selectedNode = weightedNodes[j];
                                break;
                            }
                        }
                        if (selectedNode == null) {
                            //Last node in list
                            selectedNode = weightedNodes.Last().Value;
                        }

                        //Send serialized registry
                        using (var hbContext = new ZContext())
                        using (var hbSocket = new ZSocket(hbContext, ZSocketType.DEALER)) {
                            hbSocket.Linger = TimeSpan.FromMilliseconds(50);
                            // Bind
                            //TODO: rudimentary "least hop" determination
                            hbSocket.Connect(selectedNode.ListenerAddressList[0]);
                            //hbSocket.IdentityString = gossipListenerAddress;
                            hbSocket.Send(new ZFrame(serializedRegistry.ToArray()));

                            var hbReceiver = ZPollItem.CreateReceiver();

                            System.Threading.Thread.Sleep(1);
                        }
                    }
                }
            } catch (Exception ex) {
                //TODO: Handle exception
            }

            t = new System.Timers.Timer(gossipInterval);
            t.Elapsed += GossipInterval_Elapsed;
            t.Enabled = true;
            t.Start();
        }

        /// <summary>
        /// Implementation from IDisposable
        /// </summary>
        public void Dispose() {
            queueCancellation.Cancel();
            queueProcessor.Wait();

            queueProcessor.Dispose();

            listenerCancellation.Cancel();
            listener.Wait();

            listener.Dispose();

            t.Stop();
            t.Dispose();

            //Disjoin cluster
        }
    }

    /// <summary>
    /// Internal enumeration used to identify the type of gossip message received by a remote node
    /// </summary>
    internal enum GossipMessageType {
        Heartbeat = 0,
        Join = 1,
        Leave = 2
    }
}