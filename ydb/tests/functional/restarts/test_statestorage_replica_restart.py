import logging
import time
import unittest

import requests

from ydb.tests.library.common.types import Erasure, TabletStates
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.kv.helpers import create_tablets_and_wait_for_start


logger = logging.getLogger(__name__)


class TestStateStorageReplicaRestart(unittest.TestCase):
    replica_nodes = (2, 3, 4)
    observation_seconds = 20

    def setUp(self):
        def storage_config(nodes):
            return {
                'ssid': 1,
                'ring': {
                    'nto_select': len(nodes),
                    'ring': [{'node': [node], 'use_ring_specific_node_selection': True} for node in nodes],
                },
            }

        config = KikimrConfigGenerator(
            Erasure.NONE,
            nodes=4,
            explicit_statestorage_config={
                'explicit_state_storage_config': storage_config(self.replica_nodes),
                'explicit_state_storage_board_config': storage_config((1,)),
                'explicit_scheme_board_config': storage_config((1,)),
            },
            additional_log_configs={'STATESTORAGE': LogLevels.DEBUG, 'TABLET_MAIN': LogLevels.DEBUG},
        )
        # Restart only StateStorage: all tablets and all blob-storage groups stay on node 1.
        # In particular, a single-copy blob group must not be allocated on a replica node.
        config.pdisks_info[:] = [disk for disk in config.pdisks_info if disk['node_id'] == 1]
        # Board and SchemeBoard use their own replicas on the surviving node.
        system_tablets = config.yaml_config['system_tablets']
        system_tablets['default_node'] = [1]
        for name, tablets in system_tablets.items():
            if name != 'default_node':
                for tablet in tablets:
                    tablet['node'] = [1]

        self.cluster = KiKiMR(configurator=config)
        self.addCleanup(self.cluster.stop)
        self.cluster.start(timeout_seconds=60)
        self.tablet_ids = create_tablets_and_wait_for_start(
            self.cluster.client, 3, allowed_node_ids=[1], timeout_seconds=60,
        )
        self.tablet_node_pid = self.cluster.nodes[1].pid
        self.history = []
        self.initial = self._wait_active()
        self.assertEqual({state['node'] for state in self.initial.values()}, {1}, self.initial)
        for state in self.initial.values():
            self.assertGreater(state['generation'], 0, self.initial)
        self._observe(3, 'before restart')

    def _snapshot(self):
        # Query only the surviving node. A cluster-wide lookup could time out on stopped
        # replica nodes, obscuring whether the tablets themselves are still active.
        response = requests.get(
            f'http://localhost:{self.cluster.nodes[1].mon_port}/viewer/json/tabletinfo',
            params={'node_id': 1, 'enums': 'false', 'timeout': 1000},
            timeout=5,
        )
        response.raise_for_status()
        result = {}
        for tablet in response.json().get('TabletStateInfo', []):
            tablet_id = int(tablet['TabletId'])
            if tablet_id in self.tablet_ids:
                result[tablet_id] = {
                    'generation': int(tablet.get('Generation', 0)),
                    'state': int(tablet['State']),
                    'node': int(tablet['NodeId']),
                }
        return result

    def _wait_active(self):
        deadline = time.monotonic() + 60
        snapshot = {}
        while time.monotonic() < deadline:
            snapshot = self._snapshot()
            if set(snapshot) == set(self.tablet_ids) and all(
                state['state'] == int(TabletStates.Active) for state in snapshot.values()
            ):
                return snapshot
            time.sleep(0.5)
        self.fail(f'Tablets did not recover: {snapshot}; history: {self.history}')

    def _observe(self, duration, phase):
        deadline = time.monotonic() + duration
        previous = None
        while True:
            snapshot = self._snapshot()
            if snapshot != previous:
                logger.info('%s: %s', phase, snapshot)
                self.history.append((phase, snapshot))
                previous = snapshot
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(0.5, remaining))

    def _restart_replicas(self, stopped_nodes, outage_seconds):
        self.assertTrue(set(stopped_nodes) <= set(self.replica_nodes))
        online = len(self.replica_nodes) - len(stopped_nodes)
        logger.info('Restarting SS nodes %s for %ss: %s/3 replicas remain; quorum needs 2',
                    stopped_nodes, outage_seconds, online)
        replica_pids = {node_id: self.cluster.nodes[node_id].pid for node_id in stopped_nodes}
        stopped = []
        try:
            for node_id in stopped_nodes:
                self.cluster.nodes[node_id].stop()
                stopped.append(node_id)
            self._observe(outage_seconds, f'{online}/3 SS replicas online for {outage_seconds}s')
        finally:
            for node_id in stopped:
                self.cluster.nodes[node_id].start()

        for node_id, pid in replica_pids.items():
            self.assertNotEqual(self.cluster.nodes[node_id].pid, pid, f'SS node {node_id} was not restarted')

        recovered = self._wait_active()
        self._observe(self.observation_seconds, 'after restart')
        self.assertEqual(self.cluster.nodes[1].pid, self.tablet_node_pid, 'Tablet host was restarted')
        logger.info('Before SS restart: %s; after: %s', self.initial, recovered)
        expected = {tablet_id: state['generation'] for tablet_id, state in self.initial.items()}
        actual = {tablet_id: state['generation'] for tablet_id, state in recovered.items()}
        self.assertEqual(actual, expected, f'Tablets restarted after SS restart; history: {self.history}')
        for phase, snapshot in self.history:
            for tablet_id, state in snapshot.items():
                self.assertLessEqual(
                    state['generation'], expected[tablet_id],
                    f'Tablet {tablet_id} restarted during {phase}; history: {self.history}',
                )

    def test_restart_without_quorum_loss(self):
        self._restart_replicas((2,), 20)

    def test_restart_with_quorum_loss(self):
        self._restart_replicas(self.replica_nodes, 20)

    def test_short_restart_without_quorum_loss(self):
        self._restart_replicas((2,), 5)

    def test_short_restart_with_quorum_loss(self):
        self._restart_replicas(self.replica_nodes, 5)
