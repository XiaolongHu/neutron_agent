# Copyright (c) 2013 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sqlalchemy as sa
from neutron.db import model_base
from neutron.db import models_v2
import neutron.db.api as dbapi
from oslo_log import log
from oslo_utils import timeutils

STR_LEN = 255
MAC_LEN = 16
IF_LEN = 48
IP_LEN = 48
UUID_LEN = 36
SEGTYPE_LEN = 12
ROLE_LEN = 16
TYPE_LEN = 8

LOG = log.getLogger(__name__)


class H3cHostTopology(model_base.BASEV2):
    """computer node and corresponding leaf port topology information"""
    __tablename__ = 'h3c_host_topology'

    host_name = sa.Column(sa.String(STR_LEN), primary_key=True)
    host_mac = sa.Column(sa.String(MAC_LEN), nullable=False)
    leaf_port = sa.Column(sa.String(IF_LEN), primary_key=True)
    leaf_mac = sa.Column(sa.String(MAC_LEN), primary_key=True)
    leaf_bagg = sa.Column(sa.String(IF_LEN), nullable=True)
    created_at = sa.Column(sa.DateTime, nullable=True)


class H3cDeviceTopology(model_base.BASEV2):
    """spine and leaf topology information"""
    __tablename__ = 'h3c_device_topology'

    device_mac = sa.Column(sa.String(MAC_LEN), primary_key=True)
    port = sa.Column(sa.String(IF_LEN), primary_key=True)
    peer_mac = sa.Column(sa.String(MAC_LEN), nullable=False)
    peer_port = sa.Column(sa.String(IF_LEN), nullable=False)
    port_type = sa.Column(sa.String(TYPE_LEN), nullable=False)
    role = sa.Column(sa.String(ROLE_LEN), nullable=False)
    created_at = sa.Column(sa.DateTime, nullable=True)
    bagg = sa.Column(sa.String(IF_LEN), nullable=True)


class H3cRelatedNetworks(model_base.BASEV2, models_v2.HasId,
                         models_v2.HasTenant):
    """ Representation for table comware_related_nets
        A network id corresponding a segmentation ID.
    """
    __tablename__ = 'h3c_related_nets'

    network_id = sa.Column(sa.String(UUID_LEN))
    segmentation_id = sa.Column(sa.Integer)
    segmentation_type = sa.Column(sa.String(SEGTYPE_LEN))
    created_at = sa.Column(sa.DateTime, nullable=True)


class H3cRelatedPorts(model_base.BASEV2, models_v2.HasId,
                      models_v2.HasTenant):
    """ Representation for table comware_related_vms
        This table stores all the port informations.
    """
    __tablename__ = 'h3c_related_ports'

    device_id = sa.Column(sa.String(STR_LEN))
    host_id = sa.Column(sa.String(STR_LEN))
    port_id = sa.Column(sa.String(UUID_LEN))
    network_id = sa.Column(sa.String(UUID_LEN))
    segmentation_id = sa.Column(sa.Integer)
    created_at = sa.Column(sa.DateTime, nullable=True)


class db_lib(object):
    def __init__(self):
        self.session = dbapi.get_session()
        self.smooth = False
        self.smoothTopology = {}
        self.smooth_host = {}

    def add_device_db(self, context, device_mac, port,
                      peer_mac, peer_port,
                      port_type, role, bagg=None):
        dev_db = H3cDeviceTopology(device_mac=device_mac,
                                   port=port,
                                   peer_mac=peer_mac,
                                   peer_port=peer_port,
                                   port_type=port_type,
                                   role=role,
                                   created_at=timeutils.utcnow(),
                                   bagg=bagg)
        context.session.add(dev_db)

    def create_device_topology(self, context, msg, batch):
        LOG.info("create_device_topology %s", msg)
        with context.session.begin(subtransactions=True):
            for topo in msg:
                if self.smooth is True:
                    self.smooth_topology(topo)

                query = context.session.query(H3cDeviceTopology)
                qry = query.filter_by(device_mac=topo['device_mac'],
                                      port=topo['port'])
                if qry.count() > 0:
                    qry.delete()
                self.add_device_db(context,
                                   device_mac=topo['device_mac'],
                                   port=topo['port'],
                                   peer_mac=topo['peer_mac'],
                                   peer_port=topo['peer_port'],
                                   port_type=topo['port_type'],
                                   role=topo['role'],
                                   bagg=topo.get('bagg'))

    def delete_device_topology(self, context, msg):
        """plug network cable or IP change or device replace"""
        with context.session.begin(subtransactions=True):
            for topo in msg:
                query = context.session.query(H3cDeviceTopology)
                query.filter_by(device_mac=topo['device_mac'],
                                port=topo['port']).delete()

    def update_device_topology(self, context, msg):
        with context.session.begin(subtransactions=True):
            for topo in msg:
                query = context.session.query(H3cDeviceTopology)
                query.filter_by(device_mac=topo['device_mac'],
                                port=topo['port']).delete()
                self.add_device_db(context,
                                   device_mac=topo['device_mac'],
                                   port=topo['port'],
                                   peer_mac=topo['peer_mac'],
                                   peer_port=topo['peer_port'],
                                   port_type=topo['port_type'],
                                   role=topo['role'],
                                   bagg=topo.get('bagg'))

    def del_hostdb(self, context, host_name, host_mac,
                   leaf_mac, leaf_port):
        query = context.session.query(H3cHostTopology)
        query.filter_by(host_name=host_name,
                        host_mac=host_mac,
                        leaf_mac=leaf_mac,
                        leaf_port=leaf_port).delete()

    def add_hostdb(self, context, host_name, host_mac,
                   leaf_port, leaf_mac, leaf_bagg=None):
        com_db = H3cHostTopology(host_name=host_name,
                                 host_mac=host_mac,
                                 leaf_port=leaf_port,
                                 leaf_mac=leaf_mac,
                                 leaf_bagg=leaf_bagg,
                                 created_at=timeutils.utcnow())
        context.session.add(com_db)

    def create_host_topogoly(self, context, msg, batch):
        LOG.info("create_host_topogoly %s", msg)
        with context.session.begin(subtransactions=True):
            for topo in msg:
                if self.smooth is True:
                    self.smooth_host_topology(topo)
                qry = context.session.query(H3cHostTopology). \
                    filter_by(host_name=topo['host_name'],
                              leaf_port=topo['leaf_port'],
                              leaf_mac=topo['leaf_mac'])
                if qry.count() > 0:
                    qry.delete()

                self.add_hostdb(context,
                                host_name=topo['host_name'],
                                host_mac=topo['host_mac'],
                                leaf_port=topo['leaf_port'],
                                leaf_mac=topo['leaf_mac'],
                                leaf_bagg=topo.get('bagg'))

    def delete_host_topology(self, context, msg):
        LOG.info("delete_host_topology %s", msg)
        with context.session.begin(subtransactions=True):
            for topo in msg:
                """all is equal"""
                self.del_hostdb(context,
                                host_name=topo['host_name'],
                                host_mac=topo['host_mac'],
                                leaf_mac=topo['leaf_mac'],
                                leaf_port=topo['leaf_port'])

    def update_host_topology(self, context, msg):
        with context.session.begin(subtransactions=True):
            for topo in msg:
                try:
                    context.session.query(H3cHostTopology).filter_by(
                                          host_name=topo['old_host_name'],
                                          leaf_port=topo['leaf_port'],
                                          leaf_mac=topo['leaf_mac']).delete()
                except Exception as e:
                    LOG.warn("update_host_topology delete failed %s", e)

                self.add_hostdb(context,
                                host_name=topo['host_name'],
                                host_mac=topo['host_mac'],
                                leaf_port=topo['leaf_port'],
                                leaf_mac=topo['leaf_mac'],
                                leaf_bagg=topo.get('bagg'))

    def smooth_topology(self, topo):
        if topo['device_mac'] in self.smoothTopology:
            topo_set = self.smoothTopology[topo['device_mac']]
            if topo['port'] in topo_set:
                topo_set.remove(topo['port'])

    def smooth_host_topology(self, topo):
        if topo['leaf_mac'] in self.smooth_host:
            host_set = self.smooth_host[topo['leaf_mac']]
            if topo['host_mac'] in host_set:
                host_set.remove(topo['host_mac'])

    def smoothstart_topology(self, context, mac):
        with context.session.begin(subtransactions=True):
            """device"""
            qry = (context.session.query(H3cDeviceTopology).
                   filter_by(device_mac=mac))

            self.smoothTopology[mac] = set()
            if qry.count() > 0:
                self.smooth = True
                topo_set = self.smoothTopology[mac]
                for one in qry:
                    topo_set.add(one['port'])

            """host"""
            qry = context.session.query(H3cHostTopology).filter_by(leaf_mac=mac)

            self.smooth_host[mac] = set()
            if qry.count() > 0:
                self.smooth = True
                host_set = self.smooth_host[mac]
                for one in qry:
                    host_set.add(one['host_mac'])

    def smoothend_topology(self, context, mac):
        self.smooth = False
        if mac in self.smoothTopology:
            topo_set = self.smoothTopology[mac]
            del self.smoothTopology[mac]
        else:
            topo_set = set()

        if mac in self.smooth_host:
            host_set = self.smooth_host[mac]
            del self.smooth_host[mac]
        else:
            host_set = set()

        if ((len(topo_set) == 0) and
                (len(host_set) == 0)):
            return

        with context.session.begin(subtransactions=True):
            for port in topo_set:
                (context.session.query(H3cDeviceTopology).
                 filter_by(device_mac=mac,
                           port=port).delete())
            for host_mac in host_set:
                query = context.session.query(H3cHostTopology)
                query.filter_by(leaf_mac=mac,
                                host_mac=host_mac).delete()

    def update_aggr_info(self, context, mac, net_type, role,
                         ifname, aggr_name):
        proc_host = False
        proc_dev = False
        if role == 'spine':
            if net_type == 'vlan':
                proc_dev = True
        else:
            proc_host = True
            if net_type == 'vlan':
                proc_dev = True

        with context.session.begin(subtransactions=True):
            if proc_host is True:
                """vlan or vxlan"""
                qry = context.session.query(H3cHostTopology). \
                            filter_by(leaf_mac=mac,
                                      leaf_port=ifname)
                for one in qry:
                    one.leaf_bagg = aggr_name
                    one.update(one)

            if proc_dev is True:
                """only vlan"""
                qry = context.session.query(H3cDeviceTopology). \
                            filter_by(device_mac=mac,
                                      port=ifname)
                for one in qry:
                    one.bagg = aggr_name
                    one.update(one)

    def is_network_created(self, tenant_id, network_id, seg_id=None):
        """Checks if a networks is already known to COMWARE."""
        session = self.session
        with session.begin(subtransactions=True):
            if not seg_id:
                num_nets = (session.query(H3cRelatedNetworks).
                            filter_by(tenant_id=tenant_id,
                                      network_id=network_id).count())
            else:
                num_nets = (session.query(H3cRelatedNetworks).
                            filter_by(tenant_id=tenant_id,
                                      network_id=network_id,
                                      segmentation_id=seg_id).count())

            return num_nets > 0

    def create_network(self, tenant_id, network_id, segment_id, segment_type):
        """ Store a network relationship in db. """
        session = self.session
        with session.begin(subtransactions=True):
            network = H3cRelatedNetworks(tenant_id=tenant_id,
                                         network_id=network_id,
                                         segmentation_id=segment_id,
                                         segmentation_type=segment_type,
                                         created_at=timeutils.utcnow())
            session.add(network)

    def delete_network(self, tenant_id, network_id):
        """ Remove a network relationship from comware db. """
        session = self.session
        with session.begin(subtransactions=True):
            query = session.query(H3cRelatedNetworks)
            query.filter_by(network_id=network_id).delete()

    def get_vm_host(self, port_id,
                    network_id, tenant_id,
                    segmentation_id=0):
        session = self.session
        with session.begin(subtransactions=True):
            qry = (session.query(H3cRelatedPorts).
                   filter_by(tenant_id=tenant_id,
                             port_id=port_id,
                             network_id=network_id,
                             segmentation_id=segmentation_id))
            for one in qry:
                return one['host_id']

        return None

    def is_vm_created(self, device_id, host_id, port_id,
                      network_id, tenant_id):
        """Checks if a VM is already known to comware. """
        session = self.session
        with session.begin(subtransactions=True):
            num_vm = (session.query(H3cRelatedPorts).
                      filter_by(tenant_id=tenant_id,
                                port_id=port_id,
                                network_id=network_id,
                                host_id=host_id).count())
            return num_vm > 0

    def is_vm_update(self, device_id, host_id, port_id,
                     network_id, tenant_id,
                     segmentation_id):
        session = self.session
        with session.begin(subtransactions=True):
            num_vm = (session.query(H3cRelatedPorts).
                      filter_by(tenant_id=tenant_id,
                                port_id=port_id,
                                network_id=network_id,
                                host_id=host_id,
                                segmentation_id=segmentation_id).count())
            return num_vm > 0
        return False

    def get_vm_count(self, network_id, host_id, segmentation_id):
        """ Return the number vm in the same network. """
        session = self.session
        with session.begin(subtransactions=True):
            return (session.query(H3cRelatedPorts).
                    filter_by(network_id=network_id,
                              host_id=host_id,
                              segmentation_id=segmentation_id).count())

    def is_leaf_vm_exist(self, network_id, leafmac):
        """ Return the number vm in the same network. """
        session = self.session
        with session.begin(subtransactions=True):
            query = (session.query(H3cRelatedPorts).
                     filter_by(network_id=network_id))
            for one in query:
                count = (session.query(H3cHostTopology).
                         filter_by(host_name=one['host_id'],
                                   leaf_mac=leafmac).count())
                if count > 0:
                    return True
        return False

    def update_vm(self, device_id, host_id, port_id,
                  network_id, tenant_id,
                  segmentation_id):
        session = self.session
        with session.begin(subtransactions=True):
            result = (session.query(H3cRelatedPorts).
                      filter_by(tenant_id=tenant_id,
                                device_id=device_id,
                                host_id=host_id,
                                port_id=port_id,
                                network_id=network_id))
            for vm in result:
                vm.segmentation_id = segmentation_id
                vm.update(vm)

    def create_vm(self, device_id, host_id, port_id,
                  network_id, tenant_id,
                  segmentation_id=0):
        """ Create a vm with comware. """
        session = self.session
        with session.begin(subtransactions=True):
            vm = H3cRelatedPorts(device_id=device_id,
                                 host_id=host_id,
                                 port_id=port_id,
                                 network_id=network_id,
                                 tenant_id=tenant_id,
                                 segmentation_id=segmentation_id,
                                 created_at=timeutils.utcnow())
            session.add(vm)

    def delete_vm(self, device_id, host_id, port_id,
                  network_id, tenant_id,
                  segmentation_id=0):
        """Removes all relevant information about a VM from repository."""
        session = self.session
        with session.begin(subtransactions=True):
            (session.query(H3cRelatedPorts).
                filter_by(host_id=host_id,
                          port_id=port_id, tenant_id=tenant_id,
                          network_id=network_id,
                          segmentation_id=segmentation_id).delete())

    def new_spine(self, spine_list, topo):
        for spine in spine_list:
            if spine['device_mac'] == topo['device_mac']:
                """add spine downport"""
                if topo['bagg'] is not None:
                    if topo['bagg'] not in spine['down_port']:
                        spine['down_port'].append(topo['bagg'])
                else:
                    if topo['port'] not in spine['down_port']:
                        spine['down_port'].append(topo['port'])
                return
        dev_dict = dict()
        dev_dict['device_mac'] = topo['device_mac']
        if topo['bagg'] is not None:
            dev_dict['down_port'] = [topo['bagg']]
        else:
            dev_dict['down_port'] = [topo['port']]
        spine_list.append(dev_dict)

    def new_leaf(self, leaf):
        leaf_dict = dict()
        leaf_dict['device_mac'] = leaf['leaf_mac']
        if leaf['leaf_bagg'] is not None:
            leaf_dict['down_port'] = [leaf['leaf_bagg']]
        else:
            leaf_dict['down_port'] = [leaf['leaf_port']]
        return leaf_dict

    def is_new_leaf(self, dev_dict, new_leaf):
        for one_dev in dev_dict:
            leaf = one_dev['leaf']
            if leaf['device_mac'] == new_leaf['leaf_mac']:
                return leaf
        return None

    def get_host_topology(self, host_id):
        dev_dict = []
        session = self.session
        with session.begin(subtransactions=True):
            query = (session.query(H3cHostTopology).
                     filter_by(host_name=host_id))
            for one in query:
                onetopo = {}
                """leaf"""
                old_leaf = self.is_new_leaf(dev_dict, one)
                if old_leaf is None:
                    leaf_dev = self.new_leaf(one)
                    leaf_dev['up_port'] = []
                    upport = leaf_dev['up_port']
                    onetopo['leaf'] = leaf_dev
                else:
                    if one['leaf_bagg'] is not None:
                        if one['leaf_bagg'] not in old_leaf['down_port']:
                            old_leaf['down_port'].append(one['leaf_bagg'])
                    else:
                        if one['leaf_port'] not in old_leaf['down_port']:
                            old_leaf['down_port'].append(one['leaf_port'])
                    continue

                """leaf up-port"""
                leaf_dev_topos = (session.query(H3cDeviceTopology).
                                  filter_by(device_mac=one['leaf_mac']))
                for leaf_topo in leaf_dev_topos:
                    if leaf_topo['bagg'] is not None:
                        if leaf_topo['bagg'] not in upport:
                            upport.append(leaf_topo['bagg'])
                    else:
                        if leaf_topo['port'] not in upport:
                            upport.append(leaf_topo['port'])

                """spine down-port"""
                onetopo['spine'] = []
                spine_list = onetopo['spine']
                spine_topos = (session.query(H3cDeviceTopology).
                               filter_by(peer_mac=one['leaf_mac']))
                for spine_topo in spine_topos:
                    self.new_spine(spine_list, spine_topo)
                dev_dict.append(onetopo)
        return dev_dict

    def get_segment(self, session, host_name, segments=None):
        segment = []
        network_id = set()
        vms = (session.query(H3cRelatedPorts).filter_by(host_id=host_name))
        for vm in vms:
            if len(network_id) == 0:
                network_id.add(vm['network_id'])
            elif vm['network_id'] in network_id:
                continue
            else:
                network_id.add(vm['network_id'])

            try:
                network = (session.query(H3cRelatedNetworks).
                           filter_by(tenant_id=vm['tenant_id'],
                                     network_id=vm['network_id']).one())
            except Exception as err:
                LOG.warn("host %s network:%s get_segment failed: %s",
                         host_name, vm['network_id'], err)
                continue
            if network:
                one = {}
                seg_type = network['segmentation_type']
                one['segment_type'] = seg_type
                if seg_type == 'vlan':
                    one['net_segment'] = network['segmentation_id']
                elif seg_type == 'h3c_vxlan' or seg_type == 'vxlan':
                    one['net_segment'] = network['segmentation_id']
                    one['vlanId'] = vm['segmentation_id']
                """single"""
                segment.append(one)
                """All"""
                if segments is None:
                    continue
                if len(segments) == 0:
                    segments.append(one)
                else:
                    find = False
                    for seg in segments:
                        if (seg['segment_type'] == one['segment_type'] and
                            seg['net_segment'] == one['net_segment'] and
                                seg.get('vlanId') == one.get('vlanId')):
                            find = True
                            break

                    if not find:
                        segments.append(one)
        return segment

    def get_spine_cfg(self, session, spin_mac):
        msg = dict()
        msg['role'] = 'spine'
        spines = (session.query(H3cDeviceTopology).
                 filter_by(device_mac=spin_mac))
        for spine_topo in spines:
            if spine_topo['bagg'] is not None:
                down_port = spine_topo['bagg']
            else:
                down_port = spine_topo['port']
            if down_port in msg:
                continue
            host_topos = (session.query(H3cHostTopology).
                     filter_by(leaf_mac=spine_topo['peer_mac']))
            segments = []
            msg[down_port] = segments
            for host_topo in host_topos:
                self.get_segment(session, host_topo['host_name'], segments)
        return msg

    def get_leaf_cfg(self, session, leaf_mac, net_type):
        msg = dict()
        msg['role'] = 'leaf'
        upport = []
        downports = []

        msg['upport'] = upport
        msg['up-segment'] = []
        msg['downport'] = downports
        if net_type == 'vlan':
            topos = (session.query(H3cDeviceTopology).
                     filter_by(device_mac=leaf_mac))
            for topo in topos:
                if topo['bagg']:
                    if topo['bagg'] not in upport:
                        upport.append(topo['bagg'])
                else:
                    if topo['port'] not in upport:
                        upport.append(topo['port'])
            segments = msg['up-segment']
        else:
            segments = None

        query = (session.query(H3cHostTopology).
                 filter_by(leaf_mac=leaf_mac))
        for host_topo in query:
            segment = self.get_segment(session,
                                       host_topo['host_name'],
                                       segments)
            if len(segment) == 0:
                continue
            find = False
            for one in downports:
                if host_topo['leaf_bagg'] is not None:
                    tmp_port = host_topo['leaf_bagg']
                else:
                    tmp_port = host_topo['leaf_port']
                if tmp_port in one['if']:
                    #for one_segment in segment:
                    #    one['segment'].append(one_segment)
                    find = True
                    break
            if find is True:
                continue
            downport = {}
            if host_topo['leaf_bagg'] is not None:
                downport['if'] = host_topo['leaf_bagg']
            else:
                downport['if'] = host_topo['leaf_port']
            downport['segment'] = segment
            downports.append(downport)
        return msg

    def batch_device_cfg(self, context, mac, role, net_type):
        msg = None
        with context.session.begin(subtransactions=True):
            try:
                if role == 'spine' and net_type == 'vlan':
                    msg = self.get_spine_cfg(context.session, mac)
                elif role == 'leaf':
                    msg = self.get_leaf_cfg(context.session, mac, net_type)
            except Exception as e:
                LOG.exception('batch_device_cfg error %s', e)
        return msg

    def get_vlan_by_device(self, session, topo_list, mac, is_del=False):
        msg = {}
        if mac not in self.smoothTopology or is_del is True:
            role = topo_list[0]['role']
            if role != 'spine' and role != 'leaf':
                LOG.warn("device %s role error %s", mac, topo_list)
                return msg

            msg[mac] = {}
            msg[mac]['role'] = role
            if role == 'leaf':
                upport = []
                segments = []
                msg[mac]['upport'] = upport
                msg[mac]['up-segment'] = segments
                msg[mac]['downport'] = []
            with session.begin(subtransactions=True):
                for topo in topo_list:
                    if role == 'spine':
                        if 'bagg' in topo:
                            if is_del is True:
                                count = self.get_dev_topo_count(session, topo)
                                if count > 1:
                                    down_port = topo['port']
                                else:
                                    down_port = topo['bagg']
                            else:
                                down_port = topo['bagg']
                        else:
                            down_port = topo['port']
                        segments = []
                        msg[mac][down_port] = segments
                        leaf_mac = topo['peer_mac'] 
                    else:
                        if 'bagg' in topo:
                            if is_del is True:
                                count = self.get_dev_topo_count(session, topo)
                                if count > 1:
                                    up_port = topo['port']
                                else:
                                    up_port = topo['bagg']
                            else:
                                up_port = topo['bagg']
                        else:
                            up_port = topo['port']
                        if up_port not in upport:
                            upport.append(up_port)
                        leaf_mac=topo['device_mac']

                    query = (session.query(H3cHostTopology).
                            filter_by(leaf_mac=leaf_mac))
                    for port in query:
                        self.get_segment(session, port['host_name'], segments)
        return msg

    def get_vlan_up_info(self, session, msg):
        segments = []
        up_segments = []
        del_segments = []

        for mac in msg:
            if msg[mac]['role'] == 'leaf':
                up_segments = msg[mac]['up-segment']
                if len(up_segments) == 0:
                    return
                with session.begin(subtransactions=True):
                    query = (session.query(H3cHostTopology).
                             filter_by(leaf_mac=mac))
                    for one in query:
                        self.get_segment(session, one['host_name'], segments)
                """only one leaf"""
                break
        for old in up_segments:
            if old not in segments:
                del_segments.append(old)
        for mac in msg:
            if msg[mac]['role'] == 'leaf':
                msg[mac]['up-segment'] = del_segments
            elif msg[mac]['role'] == 'spine':
                for key in msg[mac]:
                    if key != 'role':
                        msg[mac][key] = del_segments

    def get_host_vlan_by_topology(self, session, topos, mac, is_del=False):
        msg = {}
        if mac not in self.smooth_host or is_del is True:
            msg[mac] = {}
            upport = []
            downports = []
            segments = []
            msg[mac]['upport'] = upport
            msg[mac]['up-segment'] = segments
            msg[mac]['downport'] = downports
            msg[mac]['role'] = 'leaf'
            with session.begin(subtransactions=True):
                for one in topos:
                    downport = {}
                    if 'bagg' in one:
                        if is_del is True:
                            count = self.get_host_topo_count(session,
                                                             one['host_name'],
                                                             one['leaf_mac']) 
                            if count > 1:
                                """have other topology, no process"""
                                continue
                        downport['if'] = one['bagg']
                    else:
                        downport['if'] = one['leaf_port']
                    segment = self.get_segment(session,
                                               one['host_name'],
                                               segments)
                    downport['segment'] = segment
                    downports.append(downport)

                    leaf_topos = (session.query(H3cDeviceTopology).
                                 filter_by(device_mac=one['leaf_mac']))
                    for leaf_topo in leaf_topos:
                        """upport"""
                        if leaf_topo['bagg'] is not None:
                            tmp_port = leaf_topo['bagg']
                        else:
                            tmp_port = leaf_topo['port']
                        if tmp_port not in upport:
                            upport.append(tmp_port)
                        """spine"""
                        peer_mac = leaf_topo['peer_mac']
                        if peer_mac not in msg:
                            msg[peer_mac] = {}
                            msg[peer_mac]['role'] = 'spine'
                            spine_topos = (session.query(H3cDeviceTopology).
                                          filter_by(device_mac=peer_mac,
                                                    peer_mac=one['leaf_mac']))
                            for spine_topo in spine_topos:
                                if spine_topo['bagg'] is not None:
                                    tmp_port = spine_topo['bagg']
                                else:
                                    tmp_port = spine_topo['port']
                                msg[peer_mac][tmp_port] = segments
            if len(segments) == 0:
                msg = {}
        return msg

    def get_leaf_vxlan_by_topology(self, session, topos, mac, is_del=False):
        msg = {}
        if mac not in self.smooth_host or is_del is True:
            downports = []
            msg['role'] = 'leaf'
            msg['upport'] = []
            msg['up-segment'] = []
            msg['downport'] = downports
            with session.begin(subtransactions=True):
                for one in topos:
                    segment = self.get_segment(session,
                                               one['host_name'])
                    downport = {}
                    if 'bagg' in one:
                        if is_del is True:
                            count = self.get_host_topo_count(session,
                                                             one['host_name'],
                                                             one['leaf_mac']) 
                            if count > 1:
                                """have other topology, no process"""
                                continue
                        downport['if'] = one['bagg']
                    else:
                        downport['if'] = one['leaf_port']
                    downport['segment'] = segment
                    if len(segment) > 0:
                        downports.append(downport)
            if len(downports) == 0:
                msg = {}
        return msg

    def get_host_topo_count(self, session, host_name, leaf_mac):
        return (session.query(H3cHostTopology).
                filter_by(host_name=host_name,
                          leaf_mac=leaf_mac).count())

    def get_dev_topo_count(self, session, topo):
        return (session.query(H3cDeviceTopology).
                filter_by(device_mac=topo['device_mac'],
                          peer_mac=topo['peer_mac']).count())

    def aging_device_topology(self, device_mac):
        try:
            query = self.session.query(H3cDeviceTopology)
            query.filter_by(device_mac=device_mac).delete()
            LOG.info("aging device topology, device mac:%s", device_mac)
        except Exception as e:
            LOG.warn("aging device topology failed %s", e)

    def aging_host_topology(self, leaf_mac):
        try:
            query = self.session.query(H3cHostTopology)
            query.filter_by(leaf_mac=leaf_mac).delete()
            LOG.info("aging host topology, devic mac:%s", leaf_mac)
        except Exception as e:
            LOG.warn("aging host topology failed %s", e)
