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

import threading
import oslo_messaging
import db
from neutron.common import rpc as n_rpc
from neutron.plugins.ml2 import driver_api
from oslo_log import log
from neutron import context as n_context
from neutron.common import constants as n_const

from oslo_utils import timeutils
from oslo_serialization import jsonutils
from neutron.extensions import agent as ext_a
from neutron.db import model_base
from sqlalchemy.orm import exc
import sqlalchemy as sa
from sqlalchemy import sql
import neutron.db.api as dbapi

from neutron.plugins.common import constants as p_const
from neutron.plugins.ml2 import driver_api as api
from neutron.extensions import portbindings
from neutron.plugins.ml2 import db as ml2_db
from neutron.db import api as db_api
from neutron import manager
from neutron.common import exceptions as n_exc
from neutron.i18n import _LW


LOG = log.getLogger(__name__)

TOPOLOGY_EVENT_SMOOTH_START = 0x08
TOPOLOGY_EVENT_DATA = 0x09
TOPOLOGY_EVENT_SMOOTH_END = 0x0a

L3AGENT_TOPO_TYPE_LEAF = 2

STR_LEN = 255
MAC_LEN = 16
IF_LEN = 48
IP_LEN = 48
UUID_LEN = 36
SEGTYPE_LEN = 12
ROLE_LEN = 16
TYPE_LEN = 8
REPORT_INTERVAL = 60
AGENT_AGE_TIME = 1200
VLAN_DEFAULT_ID = 4097
MAX_VPN = 1024

TYPE_H3C_VXLAN = 'h3c_vxlan'


class H3CAgent(model_base.BASEV2):
    """Represents agents running in neutron deployments."""

    __tablename__ = 'h3c_agents'

    # TOPIC.host is a target topic
    host = sa.Column(sa.String(STR_LEN), primary_key=True)
    # L3 agent, DHCP agent, OVS agent, LinuxBridge
    agent_type = sa.Column(sa.String(STR_LEN), nullable=False)
    # TOPIC is a fanout exchange topic
    topic = sa.Column(sa.String(STR_LEN), nullable=False)
    admin_state_up = sa.Column(sa.Boolean, default=True,
                               server_default=sql.true(), nullable=False)
    # the time when first report came from agents
    created_at = sa.Column(sa.DateTime, nullable=False)
    # the time when first report came after agents start
    started_at = sa.Column(sa.DateTime, nullable=False)
    # updated when agents report
    heartbeat_timestamp = sa.Column(sa.DateTime, nullable=False)
    # description is note for admin user
    description = sa.Column(sa.String(255))
    # configurations: a json dict string, I think 4095 is enough
    configurations = sa.Column(sa.String(4095), nullable=False)

    @classmethod
    def is_agent_down(cls, heart_beat_time):
        return timeutils.is_older_than(heart_beat_time,
                                       75)

    @property
    def is_active(self):
        return not self.is_agent_down(self.heartbeat_timestamp)


class VpnAllocation(model_base.BASEV2):
    "Represent allocation of vpn."

    __tablename__ = 'h3c_vpn_allocations'

    vpn_id = sa.Column(sa.Integer, nullable=False, primary_key=True)
    router_id = sa.Column(sa.String(36), nullable=False)


class H3CNotifierBase(object):
    """Agent side of the openvswitch rpc API.

    API version history:
        1.0 - Initial version.
        1.1 - Added get_active_networks_info, create_dhcp_port,
              update_dhcp_port, and removed get_dhcp_port methods.

    """

    BASE_RPC_API_VERSION = '1.0'

    def __init__(self, topic):
        self.topic = topic
        target = oslo_messaging.Target(topic=topic,
                                       version=self.BASE_RPC_API_VERSION,
                                       namespace=None)

        self.client = n_rpc.get_client(target)

    def api_fanout_cast(self, context, method, msg, host=None):
        cctxt = self.client.prepare(fanout=True,
                                    topic=self.topic,
                                    server=host)
        cctxt.cast(context,
                   method,
                   msg=msg)

    def api_call(self, context, method, msg, host=None):
        cctxt = self.client.prepare(topic=self.topic, server=host)
        result = cctxt.call(context, method,
                            msg=msg)
        return result


class H3CNotifierApi(H3CNotifierBase):
    """Agent side of the openvswitch rpc API.

    API version history:
        1.0 - Initial version.
        1.1 - Added get_active_networks_info, create_dhcp_port,
              update_dhcp_port, and removed get_dhcp_port methods.

    """
    def __init__(self, topic):
        super(H3CNotifierApi, self).__init__(topic=topic)

    def create_h3c_network(self, context, msg, host=None):
        """Make a remote process call to retrieve all network info."""
        result = self.api_call(context,
                               'create_h3c_network',
                               msg,
                               host)
        return result

    def notify_host(self, context, method, msg, host):
        result = self.api_call(context,
                               method,
                               msg,
                               host)


class H3CDriver(driver_api.MechanismDriver):

    START_TIME = timeutils.utcnow()

    def __init__(self, rpc=None):
        LOG.info("H3CDriver init")
        self.sync_helper = None
        self.agent = None
        self.db = db.db_lib()
        self.context = n_context.get_admin_context_without_session()
        self.session = dbapi.get_session()
        self.vpn_id_init()
        self.rlock = threading.RLock()

    def initialize(self):
        LOG.info("H3CDriver initialize")
        self._setup_rpc()
        self.start_rpc_listeners()
        self.start_report()

    def _setup_rpc(self):
        self.notifier = H3CNotifierApi('l2-h3c-agent')

    def start_rpc_listeners(self):
        LOG.info("H3CDriver start_rpc_listeners")
        self.endpoints = [self]
        self.topic = 'q-plugin-h3c'
        self.conn = n_rpc.create_connection(new=True)
        self.conn.create_consumer(self.topic, self.endpoints,
                                  fanout=False)
        return self.conn.consume_in_threads()

    def start_report(self):
        heartbeat = threading.Timer(REPORT_INTERVAL, self.agent_aging)
        heartbeat.start()

    def _get_agent_by_type_and_host(self, agent_type, host):
        session = self.session
        with session.begin(subtransactions=True):
            query = session.query(H3CAgent)
            try:
                agent_db = query.filter_by(agent_type=agent_type,
                                           host=host).one()
                return agent_db
            except exc.NoResultFound:
                raise ext_a.AgentNotFoundByTypeHost(agent_type=agent_type,
                                                    host=host)
            except exc.MultipleResultsFound:
                raise ext_a.MultipleAgentFoundByTypeHost(agent_type=agent_type,
                                                         host=host)

    def agent_aging(self):
        session = self.session
        with session.begin(subtransactions=True):
            query = session.query(H3CAgent)
            try:
                agent_db = query.filter_by(agent_type='H3C agent',
                                           admin_state_up=True)
                for one_agent in agent_db:
                    heartbeat_timestamp = one_agent['heartbeat_timestamp']
                    time_differ = timeutils.is_older_than(heartbeat_timestamp,
                                                          AGENT_AGE_TIME)
                    if time_differ:
                        one_agent['admin_state_up'] = False
                        one_agent.update(one_agent)
                        self.db.aging_computer_by_host(one_agent['host'])
                        self.db.aging_dev_by_host(one_agent['host'])
                        LOG.info("h3c_agent %s aging success",
                                 one_agent['host'])
            except Exception as e:
                LOG.warn(_LW("h3c_agent aging failed %s"), e)
            heartbeat = threading.Timer(REPORT_INTERVAL, self.agent_aging)
            heartbeat.start()

    def _create_or_update_agent(self, context, agent):
        with context.session.begin(subtransactions=True):
            res_keys = ['agent_type', 'host', 'topic']
            res = dict((k, agent[k]) for k in res_keys)

            configurations_dict = agent.get('configurations', {})
            res['configurations'] = jsonutils.dumps(configurations_dict)

            current_time = timeutils.utcnow()
            try:
                agent_db = self._get_agent_by_type_and_host(
                    agent['agent_type'], agent['host'])
                res['heartbeat_timestamp'] = current_time
                res['admin_state_up'] = True
                if agent.get('start_flag'):
                    res['started_at'] = current_time
                agent_db.update(res)
            except ext_a.AgentNotFoundByTypeHost:
                res['created_at'] = current_time
                res['started_at'] = current_time
                res['heartbeat_timestamp'] = current_time
                res['admin_state_up'] = True
                agent_db = H3CAgent(**res)
                session = self.session
                with session.begin():
                    session.add(agent_db)

    def report_state(self, context, **kwargs):
        self.agent = context
        time = kwargs['time']
        time = timeutils.parse_strtime(time)
        if self.START_TIME > time:
            LOG.info("H3CDriver Message with invalid time stamp received")
            return

        agent_state = kwargs['agent_state']['agent_state']
        self._create_or_update_agent(context, agent_state)
        LOG.info("H3CDriver(%s) report_state", agent_state['host'])

    def get_network_info(self, context, **kwargs):
        """Retrieve and return a extended information about a network."""
        network_id = kwargs.get('msg').get('network_id')
        plugin = manager.NeutronManager.get_plugin()
        try:
            network = plugin.get_network(context, network_id)
        except n_exc.NetworkNotFound:
            LOG.warn(_LW("Network %s could not be found, it might have "
                         "been deleted concurrently."), network_id)
            return
        filters = dict(network_id=[network_id])
        network['subnets'] = plugin.get_subnets(context, filters=filters)
        network['ports'] = plugin.get_ports(context, filters=filters)
        return network

    def vpn_id_init(self):
        self.vpn_alloc = [id for id in range(MAX_VPN)]
        self.vpn_info = {}
        self.vpn_lock = threading.RLock()
        session = self.session
        with session.begin(subtransactions=True):
            query = session.query(VpnAllocation)
            for one in query:
                vpn_id = int(one['vpn_id'])
                self.vpn_info[one['router_id']] = vpn_id
                self.vpn_alloc.remove(vpn_id)

    def get_vpn_id(self, context, **kwargs):
        router_id = kwargs.get('msg').get('router_id')
        LOG.info(_LW("get_vpn_id router_id:%s"), router_id)
        vpn_id = None
        with self.vpn_lock:
            if router_id in self.vpn_info:
                return {"vpn_id": self.vpn_info[router_id]}
            else:
                vpn_id = self.vpn_alloc.pop()
                self.vpn_info[router_id] = vpn_id

        if vpn_id is None:
            return None
        session = self.session
        with session.begin(subtransactions=True):
            vpn_info = VpnAllocation(vpn_id=vpn_id,
                                     router_id=router_id)
            session.add(vpn_info)
        LOG.info(_LW("get_vpn_id vpn_id:%s"), vpn_id)
        return {"vpn_id": vpn_id}

    def free_vpn_id(self, context, **kwargs):
        router_id = kwargs.get('msg').get('router_id')
        vpn_id = kwargs.get('msg').get('vpn_id')
        if router_id is None or vpn_id is None:
            return
        LOG.info(_LW("free_vpn_id router_id:%s"), router_id)
        with self.vpn_lock:
            if router_id in self.vpn_info:
                vpn_id = self.vpn_info[router_id]
                self.vpn_alloc.append(int(vpn_id))
                del self.vpn_info[router_id]
            else:
                return

        session = self.session
        with session.begin(subtransactions=True):
            query = (session.query(VpnAllocation).
                     filter_by(vpn_id=vpn_id, router_id=router_id).delete())
        return

    def get_topo_info(self, context, **kwargs):
        LOG.info("get_topo_info msg:%s", kwargs.get('msg'))
        type = kwargs.get('msg').get('net_type')
        if L3AGENT_TOPO_TYPE_LEAF == type:
            spine_mac = kwargs.get('msg').get('spine_mac')
            return self.db.get_all_leaf(spine_mac)
        return None

    def batchport(self, mac):
        msg = self.db.get_port_cfg(mac)
        LOG.info('batchport to %s: %s', str(mac), msg)
        if msg:
            try:
                self.notifier.notify_host(self.context, 'smooth_port',
                                          msg, mac)
            except Exception as excpt:
                LOG.warn(_LW("batch_port failed to send msg to %s, err:%s"),
                         mac, excpt)

    def batch_spine_cfg(self, topo):
        mac = topo[0]['dev_mac']
        msg = self.db.get_cfg(topo, mac)
        LOG.info('batch_spine_cfg to %s: %s', str(mac), msg)
        if msg:
            try:
                self.notifier.notify_host(self.context, 'smooth_port',
                                          msg, mac)
            except Exception as e:
                LOG.warn(_LW("batch_spine_cfg failed to send msg to %s,"
                             " err:%s"), mac, e)

    def batch_leaf_cfg(self, topo):
        mac = topo[0]['leaf_mac']
        msg = self.db.get_cfg_by_leaf(topo, mac)
        LOG.info('batch_leaf_cfg to %s: %s', str(mac), msg)
        if msg:
            try:
                self.notifier.notify_host(self.context, 'smooth_port',
                                          msg, mac)
            except Exception as e:
                LOG.warn(_LW("batch_leaf_cfg failed to send msg to %s,"
                             " err:%s"), mac, e)

    def Procdevice(self, data, batch):
        change = data['del']
        if len(change) > 0:
            self.db.delete_devtopo(change)

        change = data['add']
        if len(change) > 0:
            self.db.create_devtopo(change, batch)
            self.batch_spine_cfg(change)

        change = data['mod']
        if len(change) > 0:
            self.db.update_devtopo(change)

    def Proccomputer(self, data, batch):
        change = data['del']
        if len(change) > 0:
            self.db.delete_computertopo(change)

        change = data['add']
        if len(change) > 0:
            self.db.create_computertopo(change, batch)
            self.batch_leaf_cfg(change)

        change = data['mod']
        if len(change) > 0:
            self.db.update_computertopo(change)

    def ProcessTopo(self, context, **kwargs):
        msg = kwargs['msg']
        try:
            if msg['event'] == TOPOLOGY_EVENT_DATA:
                batch = False
                if 'batch' in msg:
                    batch = True
                self.Procdevice(msg['device'], batch)
                self.Proccomputer(msg['computer'], batch)
            elif msg['event'] == TOPOLOGY_EVENT_SMOOTH_START:
                self.db.smoothstart_topo(msg['dev_mac'])
            elif msg['event'] == TOPOLOGY_EVENT_SMOOTH_END:
                self.db.smoothend_topo(msg['dev_mac'])
                self.batchport(msg['dev_mac'])
        except Exception as e:
            LOG.exception(_LW('Process topo event error %s'), e)

    def create_network_precommit(self, context):
        pass

    def create_network_postcommit(self, context):
        network = context.current
        network_id = network['id']
        tenant_id = network['tenant_id']

        LOG.info(_("Create network postcommit begin."))
        with self.rlock:
            if not self.db.is_network_created(tenant_id, network_id):
                segments = context.network_segments
                segment_id = segments[0]['segmentation_id']
                segment_type = segments[0]['network_type']
                if segment_type in ['vlan', 'vxlan', 'h3c_vxlan']:
                    self.db.create_network(tenant_id,
                                           network_id,
                                           segment_id,
                                           segment_type)
            else:
                LOG.info(_("Already related network %s with id %s, "
                           "segmentation id %s, tenant %s."),
                         str(network['name']), str(network_id),
                         str(segment_id), str(tenant_id))
        LOG.info(_("Create network postcommit end."))

    def update_network_precommit(self, context):
        pass

    def update_network_postcommit(self, context):
        pass

    def delete_network_precommit(self, context):
        pass

    def delete_network_postcommit(self, context):
        network = context.current
        network_id = network['id']
        tenant_id = network['tenant_id']

        LOG.info(_("delete network post commit begin."))
        with self.rlock:
            if self.db.is_network_created(tenant_id, network_id):
                self.db.delete_network(tenant_id, network_id)
                """vxlan notify delete"""
                segments = context.network_segments
                segment_type = segments[0]['network_type']
                if segment_type == 'h3c_vxlan':
                    msg = {}
                    msg['segment_id'] = segments[0]['segmentation_id']
                    msg['segment_type'] = segment_type
                    try:
                        LOG.info(_("Notify delete network, segment_id:%s."),
                                 msg['segment_id'])
                        self.notifier.api_fanout_cast(self.context,
                                                      'delete_network',
                                                      msg)
                    except Exception as e:
                        LOG.warn(_LW("delete_network failed to send msg,"
                                     " err:%s"), e)
        LOG.info(_("delete network post commit end."))

    def create_subnet_precommit(self, context):
        pass

    def create_subnet_postcommit(self, context):
        pass

    def update_subnet_precommit(self, context):
        pass

    def update_subnet_postcommit(self, context):
        pass

    def delete_subnet_precommit(self, context):
        pass

    def delete_subnet_postcommit(self, context):
        pass

    def create_port_precommit(self, context):
        pass

    def create_port_postcommit(self, context):
        # Here we only process virtual machine and DHCP server's port.
        port = context.current
        device_owner = port['device_owner']
        if not device_owner.startswith('compute') and \
                device_owner != n_const.DEVICE_OWNER_DHCP:
            return

        LOG.info(_("Create port begin, %s"), device_owner)
        device_id = port['device_id']
        host_id = context.host
        port_id = port['id']
        tenant_id = port['tenant_id']
        network_id = port['network_id']

        with self.rlock:
            if self.db.is_vm_created(device_id, host_id,
                                     port_id, network_id, tenant_id):
                return

            LOG.info(_("Create new VM %s with network %s on port %s host:%s"),
                     str(device_id), str(network_id),
                     str(port_id), str(host_id))
            self.db.create_vm(device_id, host_id,
                              port_id, network_id, tenant_id)

        LOG.info(_("Create port end."))

    def _is_segment_h3c_vxlan(self, segment):
        return segment[api.NETWORK_TYPE] == TYPE_H3C_VXLAN

    def _get_segments(self, top_segment, bottom_segment):
        # Return vlan segment and vxlan segment (if configured).
        if top_segment is None:
            return None, None
        elif self._is_segment_h3c_vxlan(top_segment):
            return bottom_segment, top_segment
        else:
            return top_segment, None

    def _is_vm_migrating(self, context, vlan_segment, orig_vlan_segment):
        if not vlan_segment and orig_vlan_segment:
            return (context.current.get(portbindings.HOST_ID) !=
                    context.original.get(portbindings.HOST_ID))

    def _log_missing_segment(self):
        LOG.warn("H3C: Segment is None, Event not processed.")

    def delete_port(self, host_id, port, vlanId, net_segment, network_type):
        network_id = port['network_id']
        device_id = port['device_id']
        port_id = port['id']
        tenant_id = port['tenant_id']

        LOG.info(_("delete port begin."))

        if not self.db.is_vm_created(device_id, host_id, port_id,
                                     network_id, tenant_id):
            LOG.info(_("No such vm in database, ignore it"))
            return

        vm_count = self.db.get_vm_count(network_id, host_id, vlanId)
        if vm_count > 1:
            LOG.info(_("Delete port %s, the network %s still have %d vms, "
                       "ignore this operation."),
                     str(port_id), str(network_id), vm_count)
            self.db.delete_vm(device_id, host_id, port_id,
                              network_id, tenant_id, vlanId)
            return
        else:
            LOG.info(_("Delete port %s, All VM in network %s is broken,"
                       " host:%s We need remove the configuration."),
                     str(port_id), str(network_id), str(host_id))

        msg = {}
        msg['net_segment'] = net_segment
        msg['vlanId'] = vlanId
        msg['segment_type'] = network_type

        host_topos = self.db.get_host_topo(host_id)
        self.db.delete_vm(device_id, host_id, port_id,
                          network_id, tenant_id, vlanId)
        if len(host_topos) == 0:
            LOG.info(_("no find topo with VM %s network %s port %s."),
                     str(device_id), str(network_id), str(port_id))
            return

        for topo in host_topos:
            leaftopo = topo['leaf']
            if leaftopo.get('up_port'):
                leafmac = leaftopo['dev_mac']
                if self.db.is_leaf_vm_exist(network_id, leafmac):
                    """have other hosts"""
                    LOG.info("delete_port have other host, network:%s"
                             " leafmac:%s",
                             str(network_id), str(leafmac))
                    leaftopo['up_port'] = []
                    if topo.get('spine'):
                        del topo['spine']

        msg['topo'] = host_topos
        LOG.info("Notify delete_port %s", msg)
        try:
            self.notifier.api_fanout_cast(self.context, 'delete_port', msg)
        except Exception as excpt:
            LOG.warn(_LW("delete_port failed to send msg, err:%s"), excpt)

    def update_port_precommit(self, context):
        pass

    def update_port_postcommit(self, context):
        port = context.current
        device_owner = port['device_owner']

        if not (device_owner.startswith('compute') or
                device_owner == n_const.DEVICE_OWNER_DHCP):
            return

        LOG.info(_("update port begin. %s"), device_owner)
        device_id = port['device_id']
        host_id = context.host
        port_id = port['id']
        tenant_id = port['tenant_id']
        network_id = port['network_id']
        segments = context.network.network_segments
        network_type = segments[0]['network_type']
        is_migrating = False
        old_host_id = None

        with self.rlock:
            if network_type == TYPE_H3C_VXLAN:
                vlan_segment, vxlan_segment = self._get_segments(
                    context.top_bound_segment,
                    context.bottom_bound_segment)

                orig_vlan_segment, orig_vxlan_segment = self._get_segments(
                    context.original_top_bound_segment,
                    context.original_bottom_bound_segment)

                if self._is_vm_migrating(context, vlan_segment,
                                         orig_vlan_segment):
                    vlanId = orig_vlan_segment['segmentation_id']
                    net_segment = orig_vxlan_segment['segmentation_id']
                    is_migrating = True
                    old_host_id = self.db.get_vm_host(port_id,
                                                      network_id,
                                                      tenant_id,
                                                      vlanId)
                else:
                    vlanId = vlan_segment['segmentation_id']
                    net_segment = vxlan_segment['segmentation_id']

                LOG.info("update_port_postcommit vlan_segment = %s "
                         "vxlan_segment = %s"
                         % (vlan_segment, vxlan_segment))
            else:
                net_segment = segments[0]['segmentation_id']
                vlanId = VLAN_DEFAULT_ID
                old_host_id = self.db.get_vm_host(port_id, network_id,
                                                  tenant_id, vlanId)
                if ((old_host_id is not None) and
                        (old_host_id != host_id)):
                    is_migrating = True

            if not self.db.is_vm_created(device_id, host_id, port_id,
                                         network_id, tenant_id):
                if is_migrating is False:
                    LOG.info(_("VM %s not created with network %s on port %s,"
                               " ignore"),
                             str(device_id), str(network_id), str(port_id))
                    return
                else:
                    LOG.info(_("Create new VM %s with is_migrating on port %s"
                               " host:%s"),
                             str(device_id), str(port_id), str(host_id))
                    self.db.create_vm(device_id, host_id,
                                      port_id, network_id, tenant_id)

            if self.db.is_vm_update(device_id, host_id, port_id,
                                    network_id, tenant_id, vlanId):
                LOG.info(_("VM %s is update before with port %s host:%s"),
                         str(device_id), str(port_id), str(host_id))
                return
            LOG.info(_("Update VM %s with network %s on port %s host:%s"),
                     str(device_id), str(network_id),
                     str(port_id), str(host_id))
            self.db.update_vm(device_id, host_id, port_id,
                              network_id, tenant_id, vlanId)

            """ tmp, Later modify as follows: to determine whether the same
                interface with a leaf, if not, add and then delete"""
            if is_migrating is True:
                self.delete_port(old_host_id, port, vlanId,
                                 net_segment, network_type)

            vm_num_in_network = self.db.get_vm_count(network_id,
                                                     host_id,
                                                     vlanId)
            if vm_num_in_network == 1:
                msg = {}
                msg['net_segment'] = net_segment
                msg['vlanId'] = vlanId
                msg['segment_type'] = network_type
                host_topos = self.db.get_host_topo(host_id)
                if len(host_topos) == 0:
                    LOG.info(_("no find topo with VM %s network %s port %s."),
                             str(device_id), str(network_id), str(port_id))
                    return
                msg['topo'] = host_topos
                LOG.info("Notify %s", msg)
                try:
                    self.notifier.api_fanout_cast(self.context,
                                                  'create_port',
                                                  msg)
                except Exception as e:
                    LOG.warn(_LW("update_port failed to send msg, err:%s"), e)

        LOG.info(_("update port end."))

    def delete_port_precommit(self, context):
        pass

    def delete_port_postcommit(self, context):
        """unPlug a physical host from a network.  """
        port = context.current
        device_owner = port['device_owner']

        # Only process vm device
        if not (device_owner.startswith('compute') or
                device_owner == n_const.DEVICE_OWNER_DHCP):
            return

        LOG.info(_("delete_port_postcommit begin."))
        segments = context.network.network_segments
        network_type = segments[0]['network_type']
        if network_type == TYPE_H3C_VXLAN:
            vlan_segment, vxlan_segment = self._get_segments(
                    context.top_bound_segment,
                    context.bottom_bound_segment)
            vlanId = vlan_segment['segmentation_id']
            net_segment = vxlan_segment['segmentation_id']
            LOG.info("delete_port_postcommit vlan_segment=%s vxlan_segment=%s"
                     % (vlan_segment, vxlan_segment))
        else:
            net_segment = segments[0]['segmentation_id']
            vlanId = VLAN_DEFAULT_ID

        with self.rlock:
            self.delete_port(context.host, port, vlanId,
                             net_segment, network_type)

        LOG.info(_("delete_port_postcommit end."))

    def bind_port(self, context):
        if not (context.current['device_owner'].startswith('compute') or
                context.current['device_owner'] == n_const.DEVICE_OWNER_DHCP):
            return

        LOG.info("bind_port Attempting to bind port %(port)s"
                 " on network %(network)s",
                 {'port': context.current['id'],
                  'network': context.network.current['id']})

        for segment in context.segments_to_bind:
            if self._is_segment_h3c_vxlan(segment):
                physnet = 'vlanphy'
                # Allocate dynamic vlan segment.
                vlan_segment = {api.NETWORK_TYPE: p_const.TYPE_VLAN,
                                api.PHYSICAL_NETWORK: physnet}
                context.allocate_dynamic_segment(vlan_segment)

                # Retrieve the dynamically allocated segment.
                # Database has provider_segment dictionary key.
                network_id = context.current['network_id']
                dynamic_segment = ml2_db.get_dynamic_segment(
                    db_api.get_session(), network_id, physnet)

                # Have other drivers bind the VLAN dynamic segment.
                if dynamic_segment:
                    context.continue_binding(segment[api.ID],
                                             [dynamic_segment])
                else:
                    LOG.debug("VLAN dynamic segment not allocated."
                              "VLAN dynamic segment not created for VXLAN "
                              "overlay static segment. Network segment = %s "
                              "physnet = %s" % (network_id, physnet))
            else:
                LOG.debug("No binding required for segment ID %(id)s, "
                          "segment %(seg)s, phys net %(physnet)s, and "
                          "network type %(nettype)s",
                          {'id': segment[api.ID],
                           'seg': segment[api.SEGMENTATION_ID],
                           'physnet': segment[api.PHYSICAL_NETWORK],
                           'nettype': segment[api.NETWORK_TYPE]})
