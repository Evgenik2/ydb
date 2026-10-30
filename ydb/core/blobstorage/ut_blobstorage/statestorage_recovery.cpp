#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/base/statestorage_impl.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/system/type_name.h>

namespace {

class TRecoveryTest {
public:
    TEnvironmentSetup Env;
    TTestActorSystem& Runtime;
    const ui32 NodeId;
    TIntrusivePtr<TStateStorageInfo> Info;
    ui32 TabletDeaths = 0;
    ui32 Generation = 0;
    TActorId Leader;
    TActorId UserTablet;
    decltype(TTestActorSystem::FilterFunction) PreviousFilter;

    TRecoveryTest()
        : Env({
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
            .SelfManagementConfig = true,
            .StateStorageInfoGenerator = [](std::function<TActorId(ui32, ui32)> generateId, ui32 nodeId) {
                auto info = MakeIntrusive<TStateStorageInfo>();
                auto& group = info->RingGroups.emplace_back();
                group.NToSelect = 3;
                group.Rings.resize(3);
                for (ui32 i = 0; i < 3; ++i) {
                    group.Rings[i].Replicas.push_back(generateId(nodeId, i));
                }
                return info;
            },
        })
        , Runtime(*Env.Runtime)
        , NodeId(Env.Settings.ControllerNodeId)
        , Info(Runtime.StateStorageInfoGenerator(MakeStateStorageReplicaID, NodeId))
    {
        Env.Sim(TDuration::Seconds(10));
        const auto initial = Lookup(Env.TabletId);
        Generation = initial->Get()->CurrentGeneration;
        Leader = initial->Get()->CurrentLeader;
        UserTablet = initial->Get()->CurrentLeaderTablet;
        UNIT_ASSERT(Generation && Leader && UserTablet);
        PreviousFilter = std::move(Runtime.FilterFunction);
        Runtime.FilterFunction = [this](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvTablet::TEvTabletDead::EventType
                    && ev->Get<TEvTablet::TEvTabletDead>()->TabletID == Env.TabletId) {
                ++TabletDeaths;
            }
            return !PreviousFilter || PreviousFilter(nodeId, ev);
        };
    }

    ~TRecoveryTest() {
        Runtime.FilterFunction = std::move(PreviousFilter);
    }

    TActorId Replica(ui32 index) const {
        return Info->RingGroups.front().Rings.at(index).Replicas.front();
    }

    std::unique_ptr<TEventHandle<TEvStateStorage::TEvInfo>> Lookup(ui64 tabletId,
            const TEvStateStorage::TProxyOptions& options = {}) {
        const TActorId edge = Runtime.AllocateEdgeActor(NodeId);
        Runtime.Send(new IEventHandle(MakeStateStorageProxyID(), edge,
            new TEvStateStorage::TEvLookup(tabletId, 0, options)), NodeId);
        return Runtime.WaitForEdgeActorEvent<TEvStateStorage::TEvInfo>(edge);
    }

    std::unique_ptr<TEventHandle<TEvStateStorage::TEvReplicaInfo>> ReplicaLookup(ui32 index, ui64 tabletId) {
        const TActorId edge = Runtime.AllocateEdgeActor(NodeId);
        Runtime.Send(new IEventHandle(Replica(index), edge,
            new TEvStateStorage::TEvReplicaLookup(tabletId, 0, Info->ClusterStateGeneration,
                Info->ClusterStateGuid)), NodeId);
        return Runtime.WaitForEdgeActorEvent<TEvStateStorage::TEvReplicaInfo>(edge);
    }

    void Stop(std::initializer_list<ui32> indices) {
        for (ui32 index : indices) {
            const TActorId actor = Runtime.GetNode(NodeId)->ActorSystem->LookupLocalService(Replica(index));
            UNIT_ASSERT(actor);
            Runtime.Send(new IEventHandle(actor, {}, new TEvents::TEvPoison()), NodeId);
        }
    }

    void Start(std::initializer_list<ui32> indices) {
        for (ui32 index : indices) {
            Runtime.RegisterService(Replica(index), Runtime.Register(CreateStateStorageReplica(Info, 0), NodeId));
        }
    }

    void Restart(std::initializer_list<ui32> indices) {
        Stop(indices);
        Env.Sim(TDuration::Seconds(1));
        Start(indices);
        Env.Sim(TDuration::Seconds(3));
    }

    void CheckPublished(ui64 tabletId, ui32 generation, TActorId leader, ui32 step = 0) {
        for (ui32 index = 0; index < 3; ++index) {
            const auto response = ReplicaLookup(index, tabletId);
            const auto& record = response->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(record.GetCurrentGeneration(), generation);
            UNIT_ASSERT_VALUES_EQUAL(ActorIdFromProto(record.GetCurrentLeader()), leader);
            UNIT_ASSERT_VALUES_EQUAL(record.GetCurrentStep(), step);
        }
    }

    void CheckTablet() {
        Env.FetchBaseConfig();
        UNIT_ASSERT_VALUES_EQUAL(TabletDeaths, 0);
        UNIT_ASSERT(Runtime.GetActor(Leader));
        UNIT_ASSERT(Runtime.GetActor(UserTablet));
        const auto response = Lookup(Env.TabletId);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->CurrentGeneration, Generation);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->CurrentLeader, Leader);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->CurrentLeaderTablet, UserTablet);
        CheckPublished(Env.TabletId, Generation, Leader);
    }

    void Publish(ui64 tabletId, TActorId leader, ui32 generation, ui32 step = 0) {
        const auto current = Lookup(tabletId, TEvStateStorage::TProxyOptions::SigSync);
        const TActorId edge = Runtime.AllocateEdgeActor(NodeId);
        Runtime.Send(new IEventHandle(MakeStateStorageProxyID(), edge,
            new TEvStateStorage::TEvUpdate(tabletId, 0, leader, leader, generation, step,
                current->Get()->Signature, TEvStateStorage::TProxyOptions::SigSync)), NodeId);
        const auto response = Runtime.WaitForEdgeActorEvent<TEvStateStorage::TEvInfo>(edge);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Status, NKikimrProto::OK);
        Env.Sim(TDuration::MilliSeconds(100));
        CheckPublished(tabletId, generation, leader, step);
    }
};

void CheckDelayedRecovery(TDuration delay) {
    TRecoveryTest test;
    std::set<const IEventHandle*> scheduled;
    std::map<std::pair<TActorId, ui64>, ui32> requests;
    ui32 repeatedRequests = 0;
    auto previous = std::move(test.Runtime.FilterFunction);
    test.Runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
        const auto type = ev->GetTypeRewrite();
        if (type == TEvStateStorage::TEvReplicaLookup::EventType
                || type == TEvStateStorage::TEvReplicaUpdate::EventType) {
            const auto* sender = test.Runtime.GetActor(ev->Sender);
            if (sender && TypeName(*sender).Contains("TReplicaGuardian")) {
                const ui64 cookie = type == TEvStateStorage::TEvReplicaLookup::EventType
                    ? ev->Get<TEvStateStorage::TEvReplicaLookup>()->Record.GetCookie()
                    : ev->Get<TEvStateStorage::TEvReplicaUpdate>()->Record.GetCookie();
                if (++requests[{ev->Sender, cookie}] > 1) {
                    ++repeatedRequests;
                }
            }
        }
        if (type == TEvStateStorage::TEvReplicaInfo::EventType
                && ev->Get<TEvStateStorage::TEvReplicaInfo>()->Record.GetTabletID() == test.Env.TabletId) {
            const auto* recipient = test.Runtime.GetActor(ev->GetRecipientRewrite());
            if (recipient && TypeName(*recipient).Contains("TReplicaGuardian")) {
                if (scheduled.erase(ev.get())) {
                    return !previous || previous(nodeId, ev);
                }
                scheduled.insert(ev.get());
                test.Runtime.Schedule(test.Runtime.GetClock() + delay, ev.release(), nullptr, nodeId);
                return false;
            }
        }
        return !previous || previous(nodeId, ev);
    };
    test.Restart({0, 1, 2});
    test.Env.Sim(TDuration::Seconds(20));
    test.Runtime.FilterFunction = std::move(previous);
    test.CheckTablet();
    UNIT_ASSERT_C(repeatedRequests, "Recovery did not exercise a retry of an in-flight request");
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TStateStorageRecovery) {
    Y_UNIT_TEST(ReplicaRestartWithoutQuorumLoss) {
        TRecoveryTest test;
        test.Restart({0});
        test.Env.Sim(TDuration::Seconds(20));
        test.CheckTablet();
    }

    Y_UNIT_TEST(ReplicaRestartWithQuorumLoss) {
        TRecoveryTest test;
        test.Restart({0, 1, 2});
        test.Env.Sim(TDuration::Seconds(20));
        test.CheckTablet();
    }

    Y_UNIT_TEST(RepeatedReplicaRestarts) {
        TRecoveryTest test;
        for (ui32 i = 0; i < 3; ++i) {
            test.Restart({0, 1, 2});
            test.CheckTablet();
        }
        test.Env.Sim(TDuration::Seconds(20));
        test.CheckTablet();
    }

    Y_UNIT_TEST(RecoveryWith300msReplies) {
        CheckDelayedRecovery(TDuration::MilliSeconds(300));
    }

    Y_UNIT_TEST(RecoveryWith1sReplies) {
        CheckDelayedRecovery(TDuration::Seconds(1));
    }

    Y_UNIT_TEST(RecoveryGraceExpires) {
        TRecoveryTest test;
        test.Stop({0, 1, 2});
        test.Env.Sim(TDuration::Seconds(20));
        UNIT_ASSERT_C(test.TabletDeaths, "Permanent loss must still demote the isolated tablet");
        UNIT_ASSERT(!test.Runtime.GetActor(test.Leader));
    }

    Y_UNIT_TEST(SubscriptionPreservesExistingStep) {
        TRecoveryTest test;
        constexpr ui64 tabletId = 0x123456789ULL;
        const TActorId leader = test.Runtime.AllocateEdgeActor(test.NodeId);
        test.Publish(tabletId, leader, 7, 123);
        test.Runtime.Register(CreateStateStorageTabletGuardian(tabletId, leader, leader, 7), test.NodeId);
        test.Env.Sim(TDuration::Seconds(1));
        test.CheckPublished(tabletId, 7, leader, 123);
    }

    Y_UNIT_TEST(ReplicaRestartBetweenLookupAndUpdate) {
        TRecoveryTest test;
        constexpr ui64 tabletId = 0x123456789ULL;
        const TActorId leader = test.Runtime.AllocateEdgeActor(test.NodeId);
        std::unique_ptr<IEventHandle> held;
        ui32 heldNodeId = 0;
        auto previous = std::move(test.Runtime.FilterFunction);
        test.Runtime.FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (!held && ev->GetTypeRewrite() == TEvStateStorage::TEvReplicaUpdate::EventType
                    && ev->Recipient == test.Replica(0)
                    && ev->Get<TEvStateStorage::TEvReplicaUpdate>()->Record.GetTabletID() == tabletId) {
                heldNodeId = nodeId;
                held = std::move(ev);
                return false;
            }
            return !previous || previous(nodeId, ev);
        };
        test.Runtime.Register(CreateStateStorageTabletGuardian(tabletId, leader, leader, 2), test.NodeId);
        test.Env.Sim(TDuration::Seconds(1));
        test.Runtime.FilterFunction = std::move(previous);
        UNIT_ASSERT(held);
        const ui64 oldSignature = held->Get<TEvStateStorage::TEvReplicaUpdate>()->Record.GetSignature();
        test.Stop({0});
        test.Env.Sim(TDuration::MilliSeconds(100));
        test.Start({0});
        const auto fresh = test.ReplicaLookup(0, tabletId);
        UNIT_ASSERT_VALUES_UNEQUAL(fresh->Get()->Record.GetSignature(), oldSignature);
        test.Runtime.Send(held.release(), heldNodeId);
        test.Env.Sim(TDuration::Seconds(2));
        test.CheckPublished(tabletId, 2, leader);
        test.CheckTablet();
    }

    void CheckNewOwner(ui32 currentGeneration) {
        TRecoveryTest test;
        constexpr ui64 tabletId = 0x123456789ULL;
        const TActorId owner = test.Runtime.AllocateEdgeActor(test.NodeId);
        const TActorId obsolete = test.Runtime.AllocateEdgeActor(test.NodeId);
        test.Publish(tabletId, owner, currentGeneration);
        test.Runtime.Register(CreateStateStorageTabletGuardian(tabletId, obsolete, obsolete, 2), test.NodeId);
        const auto demoted = test.Env.WaitForEdgeActorEvent<TEvTablet::TEvDemoted>(obsolete, true,
            test.Runtime.GetClock() + TDuration::Seconds(2));
        UNIT_ASSERT(demoted);
        test.CheckPublished(tabletId, currentGeneration, owner);
    }

    Y_UNIT_TEST(NewerGenerationDemotesOldLeader) {
        CheckNewOwner(3);
    }

    Y_UNIT_TEST(DifferentLeaderDemotesOldLeader) {
        CheckNewOwner(2);
    }
}
