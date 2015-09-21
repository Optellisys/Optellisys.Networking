using ProtoBuf;
using System;
using System.Collections.Generic;

namespace Optellisys.Cluster.Gossip
{
    /// <summary>
    /// Represents a node on the ring
    /// </summary>
    [ProtoContract]
    public class GossipNode
    {
        /// <summary>
        /// Constructor to initialize the node object
        /// </summary>
        public GossipNode()
        {
            NodeId = Guid.NewGuid();
            GossipHeartbeats = 0;
            SuspectMatrix = new Dictionary<Guid, bool>();
            UpdateVersion = 0;
            Suspect = false;
        }

        /// <summary>
        /// The unique ID of the node in the cluster
        /// </summary>
        [ProtoMember(1)]
        public Guid NodeId { get; set; }

        /// <summary>
        /// The full listener address (tcp://{IP}:{Port}) used by the node to listen for Gossip messages (multiple supported)
        /// </summary>
        [ProtoMember(2)]
        public string[] ListenerAddressList { get; set; }

        /// <summary>
        /// The cluster partition the node resides on
        /// </summary>
        [ProtoMember(3)]
        public string Partition { get; set; }

        /// <summary>
        /// The current status of the node
        /// </summary>
        [ProtoMember(4)]
        public GossipStatus Status { get; set; }

        /// <summary>
        /// The number of heartbeats that have elapsed since the node was last heard from
        /// </summary>
        [ProtoMember(5)]
        public int GossipHeartbeats { get; set; }

        /// <summary>
        /// A matrix indicating which nodes in the cluster suspect this node is not available
        /// </summary>
        [ProtoMember(6)]
        public Dictionary<Guid, bool> SuspectMatrix { get; set; }

        /// <summary>
        /// A version number indicating when this node was last updated
        /// </summary>
        [ProtoMember(7)]
        public long UpdateVersion { get; set; }

        /// <summary>
        /// The key used by all nodes in the cluster
        /// </summary>
        [ProtoMember(8)]
        public string ClusterKey { get; set; }

        /// <summary>
        /// The roles this node plays in the cluster
        /// </summary>
        [ProtoMember(9)]
        public string[] ClusterRoles { get; set; }

        /// <summary>
        /// The roles this node plays in the cluster
        /// </summary>
        [ProtoMember(10)]
        public string HostName { get; set; }

        /// <summary>
        /// Indicates whether this node is suspected down by the current registry holder
        /// </summary>
        [ProtoIgnore]
        public bool Suspect { get; set; }

        /// <summary>
        /// Overrides ToString for use in Hash Ring
        /// </summary>
        /// <returns></returns>
        public override string ToString() {
            return this.NodeId.ToString();
        }

        /// <summary>
        /// Overrides GetHashCode for use in Hash Ring
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode() {
            return this.NodeId.GetHashCode();
        }
    }
}
