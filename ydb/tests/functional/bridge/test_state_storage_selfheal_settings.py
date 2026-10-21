import time

from state_storage_selfheal import BridgeStateStorageSelfHeal, CONFIG_NAMES, groups, is_final, placement, replicas


class SettingsRepair(BridgeStateStorageSelfHeal):
    def test_repair_uses_configured_geometry(self):
        victims = self.choose_victims("r2", 1)
        with self.observe():
            self.stop_nodes(victims)
            after = self.wait_healed(victims)
        for _, config in self.snapshots:
            for name in CONFIG_NAMES:
                for group in groups(config, name):
                    assert len(group["Ring"]) == self.rings_count, (name, group)
                    assert all(len(ring["Node"]) == self.replicas_per_ring for ring in group["Ring"]), (name, group)
        self.wait_converged(after)
        self.bridge.write_data()


class TestBridgeSelfHealGeometryOverride(SettingsRepair):
    rings_count = 5
    replicas_per_ring = 2
    selfheal_options = {"override_rings_count": 5, "override_replicas_in_ring_count": 2}


class TestBridgeSelfHealReplicaVolume(SettingsRepair):
    rings_count = 3
    replicas_per_ring = 3
    selfheal_options = {"override_rings_count": 3, "replicas_specific_volume": 6}


class TestBridgeSelfHealSeparatedReplicas(SettingsRepair):
    expect_colocated = False
    selfheal_options = {"pileup_replicas": False}

    def assert_pile_locality(self, config):
        super().assert_pile_locality(config)
        if not is_final(config):
            return
        for pile_id in range(1, 4):
            layouts = [placement(groups(config, name, pile_id)[0]) for name in CONFIG_NAMES]
            assert len(set(layouts)) > 1, (pile_id, layouts)


class TestBridgeSelfHealDisabled(BridgeStateStorageSelfHeal):
    expect_colocated = False
    selfheal_options = {"enable": False}

    def test_disabled_sentinel_does_not_reassign_replicas(self):
        victims = self.choose_victims("r2", 2)
        with self.observe():
            self.stop_nodes(victims)
            # Seeing BAD at the root proves Sentinel's real updater is running
            # even though it is configured not to initiate State Storage repair.
            self.wait_bad_nodes(victims)
            deadline = time.monotonic() + self.relax_seconds + self.step_seconds
            while time.monotonic() < deadline:
                self.check_observer()
                time.sleep(0.5)
            for _, config in self.snapshots:
                for name in CONFIG_NAMES:
                    assert config[name] == self.initial[name], (name, config)
        for node_id in victims:
            self.bridge.cluster.nodes[node_id].start()
        self.bridge.write_data()


class TestBridgeSelfHealAllowedNodes(BridgeStateStorageSelfHeal):
    expect_colocated = False
    self_management_options = {
        "state_storage_self_heal_allowed_nodes": [node for node in range(1, 37) if node not in (31, 32, 33)],
        "state_storage_board_self_heal_allowed_nodes": [node for node in range(1, 37) if node not in (34, 35, 36)],
        "scheme_board_self_heal_allowed_nodes": [node for node in range(1, 37) if node not in (25, 26, 27)],
    }

    def assert_pile_locality(self, config):
        super().assert_pile_locality(config)
        options = {
            "StateStorageConfig": "state_storage_self_heal_allowed_nodes",
            "StateStorageBoardConfig": "state_storage_board_self_heal_allowed_nodes",
            "SchemeBoardConfig": "scheme_board_self_heal_allowed_nodes",
        }
        for name, option in options.items():
            allowed = set(self.self_management_options[option])
            for group in groups(config, name):
                assert replicas(group) <= allowed, (name, group, allowed)

    def test_repair_respects_each_subsystem_allowlist(self):
        victims = self.choose_victims("r2", 2)
        with self.observe():
            self.stop_nodes(victims)
            after = self.wait_healed(victims)
        self.wait_converged(after)
        self.bridge.write_data()


class MixedAutomaticManagement(BridgeStateStorageSelfHeal):
    expect_colocated = False

    def test_enabled_subsystems_heal_while_disabled_one_stays_unchanged(self):
        victims = self.choose_victims("r2", 2)
        with self.observe():
            self.stop_nodes(victims)
            self.wait_bad_nodes(victims)

            def enabled_healed():
                config = self.require_snapshot()
                assert is_final(config), config
                for name in set(CONFIG_NAMES) - {self.disabled_config}:
                    assert all(not replicas(group).intersection(victims) for group in groups(config, name)), (name, config)
                return config

            after = self.wait(enabled_healed, "repair of the two enabled State Storage subsystems")
            for _, config in self.snapshots:
                assert config[self.disabled_config] == self.initial[self.disabled_config], (self.disabled_config, config)
        self.wait_converged(after)
        for node_id in victims:
            self.bridge.cluster.nodes[node_id].start()
        self.bridge.write_data()


class TestBridgeStateStorageManagementDisabled(MixedAutomaticManagement):
    disabled_config = "StateStorageConfig"
    self_management_options = {"automatic_state_storage_management": False}


class TestBridgeStateStorageBoardManagementDisabled(MixedAutomaticManagement):
    disabled_config = "StateStorageBoardConfig"
    self_management_options = {"automatic_state_storage_board_management": False}


class TestBridgeSchemeBoardManagementDisabled(MixedAutomaticManagement):
    disabled_config = "SchemeBoardConfig"
    self_management_options = {"automatic_scheme_board_management": False}


class TestBridgeSelfHealRepeatedFailures(BridgeStateStorageSelfHeal):
    def test_failure_of_the_replacement_node(self):
        victims = self.choose_victims("r2", 1)
        with self.observe():
            self.stop_nodes(victims)
            first = self.wait_healed(victims)
        self.assert_reassign(self.initial, first, victims)
        self.bridge.write_data()
        replacements = [
            replicas(groups(first, name, 2)[0]) - replicas(groups(self.initial, name, 2)[0])
            for name in CONFIG_NAMES
        ]
        assert all(nodes == replacements[0] for nodes in replacements), replacements
        assert len(replacements[0]) == 1
        replacement = list(replacements[0])
        with self.observe():
            self.stop_nodes(replacement)
            second = self.wait_healed(victims + replacement)
        self.assert_reassign(first, second, replacement)
        self.wait_converged(second)
        self.bridge.write_data()

    def test_simultaneous_failures_in_different_piles(self):
        victims = self.choose_victims("r1", 1) + self.choose_victims("r2", 1)
        with self.observe():
            self.stop_nodes(victims)
            after = self.wait_healed(victims)
        self.assert_full_reconfiguration(self.initial, after)
        self.wait_converged(after)
        for pile in ("r2", "r3"):
            self.bridge.promote(pile)
            self.bridge.write_data()
