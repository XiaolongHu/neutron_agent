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

STR_LEN = 255
MAC_LEN = 16
IF_LEN = 48
IP_LEN = 48
UUID_LEN = 36
SEGTYPE_LEN = 12
ROLE_LEN = 16
TYPE_LEN = 8

LOG = log.getLogger(__name__)


class H3cComputerTopo(model_base.BASEV2):
    """connection for computer and leaf"""
    __tablename__ = 'h3c_computer_topos'

    Computer_name = sa.Column(sa.String(STR_LEN), primary_key=True)
    Computer_mac = sa.Column(sa.String(MAC_LEN), nullable=False)
    leaf_ip = sa.Column(sa.String(IP_LEN), nullable=False)
    leaf_port = sa.Column(sa.String(IF_LEN), primary_key=True)
    leaf_mac = sa.Column(sa.String(MAC_LEN), primary_key=True)
    leaf_AggPort = sa.Column(sa.String(IF_LEN), nullable=True)


class H3cDevTopo(model_base.BASEV2):
    """connection for spin and leaf"""
    __tablename__ = 'h3c_dev_topos'

    dev_mac = sa.Column(sa.String(MAC_LEN), primary_key=True)
    dev_ip = sa.Column(sa.String(IP_LEN), nullable=False)
    port = sa.Column(sa.String(IF_LEN), primary_key=True)
    peer_mac = sa.Column(sa.String(MAC_LEN), nullable=False)
    peer_port = sa.Column(sa.String(IF_LEN), nullable=False)
    peer_ip = sa.Column(sa.String(IP_LEN), nullable=True)
    peer_AggPort = sa.Column(sa.String(IF_LEN), nullable=True)
    port_type = sa.Column(sa.String(TYPE_LEN), nullable=False)
    role = sa.Column(sa.String(ROLE_LEN), nullable=False)
    AggPort = sa.Column(sa.String(IF_LEN), nullable=True)


class H3cRelatedNetworks(model_base.BASEV2, models_v2.HasId,
                         models_v2.HasTenant):
    """ Representation for table comware_related_nets
        A network id corresponding a segmentation ID.
    """
    __tablename__ = 'h3c_related_nets'

    network_id = sa.Column(sa.String(UUID_LEN))
    segmentation_id = sa.Column(sa.Integer)
    segmentation_type = sa.Column(sa.String(SEGTYPE_LEN))


class H3cRelatedVms(model_base.BASEV2, models_v2.HasId,
                    models_v2.HasTenant):
    """ Representation for table comware_related_vms
        This table stores all the VM informations.
    """
    __tablename__ = 'h3c_related_vms'

    device_id = sa.Column(sa.String(STR_LEN))
    host_id = sa.Column(sa.String(STR_LEN))
    port_id = sa.Column(sa.String(UUID_LEN))
    network_id = sa.Column(sa.String(UUID_LEN))
    segmentation_id = sa.Column(sa.Integer)


class db_lib(object):
    def __init__(self):
        self.session = dbapi.get_session()
        self.smooth = False
        self.smoothTopo = {}
        self.smoothComputer = {}

    def add_devdb(self, dev_mac, dev_ip, port,
                  peer_mac, peer_port, peer_ip,
                  peer_AggPort, port_type, role, AggPort):
        dev_db = H3cDevTopo(dev_mac=dev_mac,
                            dev_ip=dev_ip,
                            port=port,
                            peer_mac=peer_mac,
                            peer_port=peer_port,
                            peer_ip=peer_ip,
                            peer_AggPort=peer_AggPort,
                            port_type=port_type,
                            role=role,
                            AggPort=AggPort)
        self.session.add(dev_db)

    def create_devtopo(self, msg, batch):
        LOG.info("add_devtopo %s", msg)
        with self.session.begin(subtransactions=True):
            for topo in msg:
                if self.smooth is True:
                    self.smooth_topo(topo)

                query = self.session.query(H3cDevTopo)
                qry = query.filter_by(dev_mac=topo['dev_mac'],
                                      port=topo['port'])
                if qry.count() > 0:
                    qry.delete()
                self.add_devdb(dev_mac=topo['dev_mac'],
                               dev_ip=topo['dev_ip'],
                               port=topo['port'],
                               peer_mac=topo['peer_mac'],
                               peer_port=topo['peer_port'],
                               peer_ip=topo['peer_ip'],
                               peer_AggPort=topo['peer_AggPort'],
                               port_type=topo['port_type'],
                               role=topo['role'],
                               AggPort=topo['AggPort'])

    def delete_devtopo(self, msg):
        """plug network cable or IP change or device replace"""
        with self.session.begin(subtransactions=True):
            for topo in msg:
                query = self.session.query(H3cDevTopo)
                query.filter_by(dev_mac=topo['dev_mac'],
                                port=topo['port']).delete()

    def update_devtopo(self, msg):
        with self.session.begin(subtransactions=True):
            for topo in msg:
                query = self.session.query(H3cDevTopo)
                query.filter_by(dev_mac=topo['dev_mac'],
                                port=topo['port']).delete()
                self.add_devdb(dev_mac=topo['dev_mac'],
                               dev_ip=topo['dev_ip'],
                               port=topo['port'],
                               peer_mac=topo['peer_mac'],
                               peer_port=topo['peer_port'],
                               peer_ip=topo['peer_ip'],
                               peer_AggPort=topo['peer_AggPort'],
                               port_type=topo['port_type'],
                               role=topo['role'],
                               AggPort=topo['AggPort'])

    def del_computerdb(self, Computer_name, Computer_mac,
                       leaf_mac, leaf_port):
        query = self.session.query(H3cComputerTopo)
        query.filter_by(Computer_name=Computer_name,
                        Computer_mac=Computer_mac,
                        leaf_mac=leaf_mac,
                        leaf_port=leaf_port).delete()

    def add_computerdb(self, Computer_name, Computer_mac,
                       leaf_ip, leaf_port, leaf_mac, leaf_AggPort):
        com_db = H3cComputerTopo(Computer_name=Computer_name,
                                 Computer_mac=Computer_mac,
                                 leaf_ip=leaf_ip,
                                 leaf_port=leaf_port,
                                 leaf_mac=leaf_mac,
                                 leaf_AggPort=leaf_AggPort)
        self.session.add(com_db)

    def create_computertopo(self, msg, batch):
        LOG.info("add_computertopo %s", msg)
        with self.session.begin(subtransactions=True):
            for topo in msg:
                if self.smooth is True:
                    self.smooth_computer(topo)
                qry = self.session.query(H3cComputerTopo). \
                    filter_by(Computer_name=topo['Computer_name'],
                              leaf_port=topo['leaf_port'],
                              leaf_mac=topo['leaf_mac'])
                if qry.count() > 0:
                    qry.delete()

                self.add_computerdb(Computer_name=topo['Computer_name'],
                                    Computer_mac=topo['Computer_mac'],
                                    leaf_ip=topo['leaf_ip'],
                                    leaf_port=topo['leaf_port'],
                                    leaf_mac=topo['leaf_mac'],
                                    leaf_AggPort=topo['leaf_AggPort'])

    def delete_computertopo(self, msg):
        with self.session.begin(subtransactions=True):
            for topo in msg:
                """all is equal"""
                self.del_computerdb(Computer_name=topo['Computer_name'],
                                    Computer_mac=topo['Computer_mac'],
                                    leaf_mac=topo['leaf_mac'],
                                    leaf_port=topo['leaf_port'])

    def update_computertopo(self, msg):
        with self.session.begin(subtransactions=True):
            for topo in msg:
                try:
                    self.session.query(H3cComputerTopo).filter_by(
                                Computer_name=topo['old_Computer_name'],
                                leaf_port=topo['leaf_port'],
                                leaf_mac=topo['leaf_mac'],).delete()
                except Exception as e:
                    LOG.warn(_LW("update_computertopo delete failed %s"), e)

                self.add_computerdb(Computer_name=topo['Computer_name'],
                                    Computer_mac=topo['Computer_mac'],
                                    leaf_ip=topo['leaf_ip'],
                                    leaf_port=topo['leaf_port'],
                                    leaf_mac=topo['leaf_mac'],
                                    leaf_AggPort=topo['leaf_AggPort'])

    def smooth_topo(self, topo):
        if topo['dev_mac'] in self.smoothTopo:
            topo_set = self.smoothTopo[topo['dev_mac']]
            if topo['port'] in topo_set:
                topo_set.remove(topo['port'])

    def smooth_computer(self, topo):
        if topo['leaf_mac'] in self.smoothComputer:
            Computer_set = self.smoothComputer[topo['leaf_mac']]
            if topo['Computer_mac'] in Computer_set:
                Computer_set.remove(topo['Computer_mac'])

    def smoothstart_topo(self, mac):
        with self.session.begin(subtransactions=True):
            """dev"""
            qry = self.session.query(H3cDevTopo).filter_by(dev_mac=mac)

            self.smoothTopo[mac] = set()
            if qry.count() > 0:
                self.smooth = True
                topo_set = self.smoothTopo[mac]
                for one in qry:
                    topo_set.add(one['port'])

            """computer"""
            qry = self.session.query(H3cComputerTopo).filter_by(leaf_mac=mac)

            self.smoothComputer[mac] = set()
            if qry.count() > 0:
                self.smooth = True
                computer_set = self.smoothComputer[mac]
                for one in qry:
                    computer_set.add(one['Computer_mac'])

    def smoothend_topo(self, mac):
        self.smooth = False
        if mac in self.smoothTopo:
            topo_set = self.smoothTopo[mac]
            del self.smoothTopo[mac]
        else:
            topo_set = set()

        if mac in self.smoothComputer:
            computer_set = self.smoothComputer[mac]
            del self.smoothComputer[mac]
        else:
            computer_set = set()

        if ((len(topo_set) == 0) and
                (len(computer_set) == 0)):
            return

        with self.session.begin(subtransactions=True):
            for port in topo_set:
                (self.session.query(H3cDevTopo).filter_by(dev_mac=mac,
                                                          port=port).delete())
            for Computer_mac in computer_set:
                query = self.session.query(H3cComputerTopo)
                query.filter_by(leaf_mac=mac,
                                Computer_mac=Computer_mac).delete()

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
                                         segmentation_type=segment_type)
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
            qry = (session.query(H3cRelatedVms).
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
        num_vm = 0
        with session.begin(subtransactions=True):
            num_vm = (session.query(H3cRelatedVms).
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
            num_vm = (session.query(H3cRelatedVms).
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
            return (session.query(H3cRelatedVms).
                    filter_by(network_id=network_id,
                              host_id=host_id,
                              segmentation_id=segmentation_id).count())

    def is_leaf_vm_exist(self, network_id, leafmac):
        """ Return the number vm in the same network. """
        session = self.session
        with session.begin(subtransactions=True):
            query = (session.query(H3cRelatedVms).
                     filter_by(network_id=network_id))
            for one in query:
                count = (session.query(H3cComputerTopo).
                         filter_by(Computer_name=one['host_id'],
                                   leaf_mac=leafmac).count())
                if count > 0:
                    return True
        return False

    def update_vm(self, device_id, host_id, port_id,
                  network_id, tenant_id,
                  segmentation_id):
        session = self.session
        with session.begin(subtransactions=True):
            result = (session.query(H3cRelatedVms).
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
            vm = H3cRelatedVms(device_id=device_id,
                               host_id=host_id,
                               port_id=port_id,
                               network_id=network_id,
                               tenant_id=tenant_id,
                               segmentation_id=segmentation_id)
            session.add(vm)

    def delete_vm(self, device_id, host_id, port_id,
                  network_id, tenant_id,
                  segmentation_id=0):
        """Removes all relevant information about a VM from repository."""
        session = self.session
        with session.begin(subtransactions=True):
            (session.query(H3cRelatedVms).
                filter_by(host_id=host_id,
                          port_id=port_id, tenant_id=tenant_id,
                          network_id=network_id,
                          segmentation_id=segmentation_id).delete())

    def new_spine(self, spine_list, dev, upport):
        for one in spine_list:
            if ((one['dev_mac'] == dev['dev_mac']) and
                    (one['dev_ip'] == dev['dev_ip'])):
                """add spine downport"""
                one['down_port'].append(dev['port'])
                """add leaf upport"""
                upport.append(dev['peer_port'])
                return
        dev_dict = {}
        dev_dict['dev_mac'] = dev['dev_mac']
        dev_dict['dev_ip'] = dev['dev_ip']
        dev_dict['down_port'] = []
        dev_dict['down_port'].append(dev['port'])
        spine_list.append(dev_dict)
        """leaf upport"""
        upport.append(dev['peer_port'])

    def new_leaf(self, leaf):
        leaf_dict = {}
        leaf_dict['dev_mac'] = leaf['leaf_mac']
        leaf_dict['dev_ip'] = leaf['leaf_ip']
        leaf_dict['down_port'] = [leaf['leaf_port']]
        return leaf_dict

    def is_new_leaf(self, dev_dict, new_leaf):
        for one_dev in dev_dict:
            leaf = one_dev['leaf']
            if ((leaf['dev_mac'] == new_leaf['leaf_mac']) and
                    (leaf['dev_ip'] == new_leaf['leaf_ip'])):
                return leaf
        return None

    def get_host_topo(self, host_id):
        dev_dict = []

        session = self.session
        with session.begin(subtransactions=True):
            query = (session.query(H3cComputerTopo).
                     filter_by(Computer_name=host_id))
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
                    old_leaf['down_port'].append(one['leaf_port'])
                    continue

                """spine, leaf up-port"""
                onetopo['spine'] = []
                spine_list = onetopo['spine']
                spines = (session.query(H3cDevTopo).
                          filter_by(peer_mac=one['leaf_mac']))
                for spine in spines:
                    self.new_spine(spine_list, spine, upport)
                dev_dict.append(onetopo)

        return dev_dict

    def get_segment(self, session, host, segments):
        segment = []
        network_id = set()
        vms = (session.query(H3cRelatedVms).
               filter_by(host_id=host['Computer_name']))
        for vm in vms:
            if len(network_id) == 0:
                network_id.add(vm['network_id'])
            elif vm['network_id'] in network_id:
                continue
            else:
                network_id.add(vm['network_id'])

            net = (session.query(H3cRelatedNetworks).
                   filter_by(tenant_id=vm['tenant_id'],
                             network_id=vm['network_id']).one())
            if net:
                one = {}
                type = net['segmentation_type']
                one['segment_type'] = type
                if type == 'vlan':
                    one['net_segment'] = net['segmentation_id']
                elif type == 'h3c_vxlan':
                    one['net_segment'] = net['segmentation_id']
                    one['vlanId'] = vm['segmentation_id']
                elif type == 'vxlan':
                    pass
                """single"""
                segment.append(one)
                """All"""
                if len(segments) == 0:
                    segments.append(one)
                else:
                    find = False
                    for seg in segments:
                        if ((seg['segment_type'] == one['segment_type']) and
                            (seg['net_segment'] == one['net_segment']) and
                                (seg.get('vlanId') == one.get('vlanId'))):
                            find = True
                            break

                    if not find:
                        segments.append(one)
        return segment

    def get_spine_cfg(self, session, spin_mac):
        msg = {}
        msg['role'] = 'spine'
        leafs = (session.query(H3cDevTopo).
                 filter_by(dev_mac=spin_mac))
        for leaf in leafs:
            hosts = (session.query(H3cComputerTopo).
                     filter_by(leaf_mac=leaf['peer_mac']))
            segments = []
            msg[leaf['port']] = segments
            for host in hosts:
                self.get_segment(session, host, segments)

        return msg

    def get_leaf_cfg(self, session, leaf_mac):
        msg = {}
        msg['role'] = 'leaf'
        upport = []
        downports = []
        segments = []

        ports = (session.query(H3cDevTopo).
                 filter_by(dev_mac=leaf_mac))
        for port in ports:
            upport.append(port['port'])
        msg['upport'] = upport
        msg['up-segment'] = segments
        msg['downport'] = downports

        query = (session.query(H3cComputerTopo).
                 filter_by(leaf_mac=leaf_mac))
        for host in query:
            segment = self.get_segment(session, host, segments)
            """if len(segment) == 0:
                continue"""
            find = False
            for tmpport in downports:
                if tmpport['if'] == host['leaf_port']:
                    for one in segment:
                        tmpport['segment'].append(one)
                    find = True
                    break
            if find:
                continue
            downport = {}
            downport['if'] = host['leaf_port']
            downport['segment'] = segment
            downports.append(downport)

        return msg

    def get_port_cfg(self, mac):
        msg = None
        role = None
        session = self.session
        with session.begin(subtransactions=True):
            query_dev = (session.query(H3cDevTopo).
                         filter_by(dev_mac=mac))
            if query_dev.count() == 0:
                query_host = (session.query(H3cComputerTopo).
                              filter_by(leaf_mac=mac))
                if query_host.count() > 0:
                    role = 'leaf'
            else:
                for one in query_dev:
                    role = one['role']
                    break

            try:
                if role == 'spine':
                    msg = self.get_spine_cfg(session, mac)
                elif role == 'leaf':
                    msg = self.get_leaf_cfg(session, mac)
            except Exception as e:
                LOG.warn(_LW('get_port_cfg error %s'), e)
        return msg

    def get_all_leaf(self, spine_mac):
        msg = []
        session = self.session
        with session.begin(subtransactions=True):
            query = (session.query(H3cDevTopo).
                     filter_by(dev_mac=spine_mac,
                               role='spine'))
            for one in query:
                leaf = {
                        'leafip': one['peer_ip'],
                        'leafmac': one['peer_mac']
                        }
                msg.append(leaf)
        return msg

    def get_cfg(self, topo, mac):
        msg = None
        session = self.session
        if mac not in self.smoothTopo:
            if topo[0]['role'] == 'spine':
                msg = self.get_cfg_by_spine(session, topo)

        return msg

    def get_cfg_by_spine(self, session, topo):
        msg = {}
        msg['role'] = 'spine'
        with session.begin(subtransactions=True):
            for host in topo:
                query = (session.query(H3cComputerTopo).
                         filter_by(leaf_mac=host['peer_mac']))
                segments = []
                msg[host['port']] = segments
                for one in query:
                    self.get_segment(session, one, segments)

        return msg

    def get_cfg_by_leaf(self, topo, mac):
        msg = None
        if mac not in self.smoothComputer:
            msg = {}
            msg['role'] = 'leaf'
            upport = []
            downports = []
            segments = []
            msg['upport'] = upport
            msg['up-segment'] = segments
            msg['downport'] = downports
            session = self.session
            with session.begin(subtransactions=True):
                for one in topo:
                    ports = (session.query(H3cDevTopo).
                             filter_by(dev_mac=one['leaf_mac']))
                    for port in ports:
                        if port['port'] not in upport:
                            upport.append(port['port'])

                    segment = self.get_segment(session, one, segments)
                    """if len(segment) == 0:
                    continue"""
                    downport = {}
                    downport['if'] = one['leaf_port']
                    downport['segment'] = segment
                    downports.append(downport)

        return msg

    def aging_dev_by_host(self, dev_mac):
        try:
            query = self.session.query(H3cDevTopo)
            query.filter_by(dev_mac=dev_mac).delete()
            LOG.info("dev_topo aging session:%s", self.session)
        except Exception as e:
            LOG.info("dev_topo[%s] aging failed", e)

    def aging_computer_by_host(self, leaf_mac):
        try:
            query = self.session.query(H3cComputerTopo)
            query.filter_by(leaf_mac=leaf_mac).delete()
            LOG.info("computer_topo aging session:%s", self.session)
        except Exception as e:
            LOG.info("computer_topo[%s] aging failed", e)
