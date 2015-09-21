using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;

namespace Optellisys.Utilities {

    public static class EndpointParser {

        /// <summary>
        /// Parses a string of endpoints into a list of IPEndpoint objects
        /// </summary>
        /// <param name="endpointList"></param>
        /// <returns></returns>
        public static List<IPEndPoint> ParseEndpoints(string endpointList) {
            List<IPEndPoint> result = new List<IPEndPoint>();

            Dictionary<IPAddress, List<int>> includeList = new Dictionary<IPAddress, List<int>>();
            List<Tuple<IPAddress, List<int>, bool, bool>> excludeList = new List<Tuple<IPAddress, List<int>, bool, bool>>();

            IEnumerable<string> epRules = endpointList.Split(';').Where(e => !string.IsNullOrEmpty(e));

            //Address Ranges
            //    << IP >>:<< Port >> - Specific IP / Port
            //    *:<< Port >> - Specific port on all addresses
            //    << IP >>:<< StartPort >> - << EndPort >> - Port range on specific address
            //    << IP >>:<< Port1 >>,<< Port2 >> - Port list on specific address
            //    !<< IP >>:!<<Port>> - Exclude address (port is ignored)
            //    << IP >>:!<< StartPort >> -<< EndPort >> - Exclude port from specific address
            //    *:!<< StartPort >> -<< EndPort >> - Exclude port range from all addresses

            foreach (var i in epRules) {
                Dictionary<IPAddress, List<int>> ruleList = new Dictionary<IPAddress, List<int>>();

                string[] rule = i.Split(':');
                if (rule.Length == 2) {
                    string ruleAddress = string.Empty, rulePorts = string.Empty;

                    bool excludeAddresses = false;
                    bool excludePorts = false;
                    if (rule[0].Trim().StartsWith("!")) {
                        ruleAddress = rule[0].Trim().Replace("!", "");
                        excludeAddresses = true;
                    } else {
                        ruleAddress = rule[0].Trim();
                    }
                    if (rule[1].Trim().StartsWith("!")) {
                        rulePorts = rule[1].Trim().Replace("!", "");
                        excludePorts = true;
                    } else {
                        rulePorts = rule[1].Trim();
                    }

                    //Generate/Parse IP address
                    if (ruleAddress.Equals("*")) { //All locally assigned network IP addreses
                        IPAddress[] l = Dns.GetHostEntry(Dns.GetHostName()).AddressList.Where(ip => ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork).ToArray();
                        foreach (var j in l) {
                            ruleList.Add(j, new List<int>());
                        }
                    } else { //Specific IP Address
                        IPAddress j = null;
                        if (IPAddress.TryParse(ruleAddress, out j)) {
                            ruleList.Add(j, new List<int>());
                        } // else malformed ip address
                    }

                    //Parse ports
                    if (rulePorts.Contains('-')) { //Port Range
                        string[] portStartEnd = rulePorts.Split('-');
                        if (portStartEnd.Length == 2) {
                            int portStart = 0, portEnd = 0;
                            if (!int.TryParse(portStartEnd[0].Trim(), out portStart)) {
                                // malformed port start
                                continue;
                            }
                            if (!int.TryParse(portStartEnd[1].Trim(), out portEnd)) {
                                // malformed port end
                                continue;
                            }
                            if (portStart > portEnd) {
                                // invalid port range
                            }
                            for (var p = portStart; p <= portEnd; p++) {
                                foreach (var key in ruleList.Keys) {
                                    ruleList[key].Add(p);
                                }
                            }
                        } // else malformed port range
                    } else if (rulePorts.Contains(',')) { //Port List
                        IEnumerable<string> portList = rulePorts.Split(',').Where(s => !string.IsNullOrEmpty(s));
                        if (portList.Count() == 0) {
                            //invalid port list
                            continue;
                        }
                        foreach (var p in portList) {
                            int port = 0;
                            if (!int.TryParse(p.Trim(), out port)) {
                                //invalid port in port list
                                continue;
                            }
                            foreach (var key in ruleList.Keys) {
                                ruleList[key].Add(port);
                            }
                        }
                    } else { //Specific Port
                        if (!string.IsNullOrEmpty(rulePorts)) {
                            int port = 0;
                            if (!int.TryParse(rulePorts, out port)) {
                                //invalid port declaration
                                continue;
                            }
                            foreach (var key in ruleList.Keys) {
                                ruleList[key].Add(port);
                            }
                        }  // else either full address exclude or invalid port declaration
                    }

                    if (!excludeAddresses && !excludePorts) {
                        foreach (var k in ruleList.Keys) {
                            //Add ip addresses to working list
                            if (!includeList.ContainsKey(k)) {
                                includeList.Add(k, new List<int>());
                            }
                            //Add ports to ip address working list
                            foreach (int p in ruleList[k]) {
                                if (!includeList[k].Contains(p)) {
                                    includeList[k].Add(p);
                                }
                            }
                        }
                    } else {
                        foreach (var k in ruleList.Keys) {
                            //Add ip addresses to working list
                            if (!excludeList.Any(t => t.Item1.Equals(k))) {
                                excludeList.Add(new Tuple<IPAddress, List<int>, bool, bool>(k, new List<int>(), excludeAddresses, excludePorts));
                            }
                            //Add ports to ip address working list
                            List<int> portRef = excludeList.FirstOrDefault(t => t.Item1.Equals(k)).Item2;
                            foreach (int p in ruleList[k]) {
                                if (!portRef.Contains(p)) {
                                    portRef.Add(p);
                                }
                            }
                        }
                    }
                } // else malformed endpoint rule
            } //end foreach{}

            //Merge include/exclude lists
            foreach (var t in excludeList) {
                if (t.Item3) { //Exclude entire address
                    if (includeList.ContainsKey(t.Item1)) {
                        includeList.Remove(t.Item1);
                    }
                } else if (t.Item4) {
                    if (includeList.ContainsKey(t.Item1)) {
                        List<int> portRef = includeList[t.Item1];
                        foreach (var p in t.Item2) {
                            if (portRef.Contains(p)) {
                                portRef.Remove(p);
                            }
                        }
                    }
                }
            }

            //Inflate Endpoints
            foreach (var a in includeList.Keys) {
                foreach (var p in includeList[a]) {
                    result.Add(new IPEndPoint(a, p));
                }
            }

            return result;
        }
    }
}