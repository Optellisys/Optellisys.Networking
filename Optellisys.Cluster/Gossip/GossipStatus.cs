using ProtoBuf;

namespace Optellisys.Cluster.Gossip {
    /// <summary>
    /// Enumeration of possible node statuses
    /// </summary>
    [ProtoContract]
    public enum GossipStatus {
        /// <summary>
        /// New or never a member of the cluster
        /// </summary>
        Unknown = 0,

        /// <summary>
        /// Node is active and responding
        /// </summary>
        Active = 1,

        /// <summary>
        /// Node is suspected of failure or latency
        /// </summary>
        Suspect = 2,

        /// <summary>
        /// Node is removed and suspended
        /// </summary>
        Suspended = 3
    }
}
