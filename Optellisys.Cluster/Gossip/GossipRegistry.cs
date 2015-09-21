using ProtoBuf;
using System.Collections.Generic;

namespace Optellisys.Cluster.Gossip
{
    /// <summary>
    /// An envelope class for handling the delivery of cluster status messages
    /// </summary>
    [ProtoContract]
    public class GossipRegistry
    {
        /// <summary>
        /// A list of nodes currently known by the cluster
        /// </summary>
        [ProtoMember(1)]
        public List<GossipNode> GossipList { get; set; }

        /// <summary>
        /// The cluster key used to authenticate the message on the receiving node
        /// </summary>
        [ProtoMember(2)]
        public string ClusterKey { get; set; }

    }
}
