#include "distconf_invoke.h"
#include "node_warden_impl.h"

#include <ydb/core/mind/bscontroller/bsc.h>

#include <ydb/library/yaml_config/yaml_config_parser.h>
#include <ydb/library/yaml_config/util.h>
#include <ydb/library/yaml_json/yaml_to_json.h>
#include <library/cpp/protobuf/json/proto2json.h>

namespace NKikimr::NStorage {

    using TInvokeRequestHandlerActor = TDistributedConfigKeeper::TInvokeRequestHandlerActor;

    namespace {
        YAML::Node Json2Yaml(const NJson::TJsonValue& json) {
            YAML::Node res;

            switch (json.GetType()) {
                case NJson::JSON_UNDEFINED: return YAML::Node();
                case NJson::JSON_NULL:      return YAML::Node(YAML::NodeType::Null);
                case NJson::JSON_BOOLEAN:   return YAML::Node(json.GetBoolean());
                case NJson::JSON_INTEGER:   return YAML::Node(json.GetInteger());
                case NJson::JSON_DOUBLE:    return YAML::Node(json.GetDouble());
                case NJson::JSON_STRING:    return YAML::Node(json.GetString());
                case NJson::JSON_UINTEGER:  return YAML::Node(json.GetUInteger());

                case NJson::JSON_MAP:
                    res = YAML::Node(YAML::NodeType::Map);
                    for (const auto& [key, value] : json.GetMap()) {
                        res.force_insert(key, Json2Yaml(value));
                    }
                    return res;

                case NJson::JSON_ARRAY:
                    res = YAML::Node(YAML::NodeType::Sequence);
                    for (const auto& item : json.GetArray()) {
                        res.push_back(Json2Yaml(item));
                    }
                    return res;
            }

            Y_ABORT();
        }
    }

    void TInvokeRequestHandlerActor::FetchStorageConfig(bool fetchMain, bool fetchStorage, bool addExplicitMgmtSections,
            bool addV1) {
        RunCommonChecks();

        if (!Self->MainConfigYaml) {
            throw TExError() << "No stored YAML for storage config";
        }

        Finish(TResult::OK, std::nullopt, [&](TResult *record) {
            auto *res = record->MutableFetchStorageConfig();
            if (fetchMain) {
                res->SetYAML(Self->MainConfigYaml);
            }
            if (fetchStorage && Self->StorageConfigYaml) {
                res->SetStorageYAML(*Self->StorageConfigYaml);
            }

            if (addExplicitMgmtSections && addV1) {
                throw TExError() << "Can't provide both explicit sections and config suitable for downgrade to v1";
            } else if (Self->StorageConfigYaml && addV1) {
                throw TExError() << "Can't downgrade to v1 when dedicated storage section is enabled";
            } else if (addExplicitMgmtSections && Self->StorageConfigYaml && !fetchStorage) {
                throw TExError() << "Can't add explicit sections to storage config as it is not fetched";
            }

            auto enrich = [&](auto&& protobuf, auto&& callback) {
                TString *yaml = nullptr;

                if (res->HasStorageYAML()) {
                    yaml = res->MutableStorageYAML();
                } else if (res->HasYAML()) {
                    yaml = res->MutableYAML();
                } else {
                    throw TExError() << "Can't add explicit sections to main config as it is not fetched";
                }

                auto doc = NFyaml::TDocument::Parse(*yaml);

                NJson::TJsonValue json;
                NProtobufJson::Proto2Json(protobuf, json, NYamlConfig::GetProto2JsonConfig());
                auto inserted = NFyaml::TDocument::Parse(YAML::Dump(Json2Yaml(json)));
                callback(doc.Root(), inserted.Root().Copy(doc), doc);
                *yaml = TString(doc.EmitToCharArray().get());
            };

            auto replace = [&](const char *section) {
                return [section](auto rootNode, auto protoNode, auto& doc) {
                    auto m = rootNode.Map().at("config").Map();
                    if (auto ref = m.pair_at_opt(section)) {
                        m.Remove(ref);
                    }
                    m.Append(doc.CreateScalar(section), protoNode);
                };
            };

            if (addExplicitMgmtSections || addV1) { // add BlobStorageConfig (or replace existing one)
                auto bsConfig = Self->StorageConfig->GetBlobStorageConfig();
                bsConfig.ClearDefineHostConfig();
                bsConfig.ClearDefineBox();
                enrich(bsConfig, replace("blob_storage_config"));
            }
            if (addExplicitMgmtSections) {
                auto callback = [&](const char *name) {
                    return [name](auto rootNode, auto protoNode, auto& doc) {
                        auto m = rootNode.Map().at("config").Map();
                        if (!m.Has("domains_config")) {
                            m.Append(doc.CreateScalar("domains_config"), doc.CreateMapping());
                        }

                        m = m["domains_config"].Map();
                        if (auto ref = m.pair_at_opt(name)) {
                            m.Remove(ref);
                        }
                        m.Append(doc.CreateScalar(name), protoNode);
                    };
                };
                enrich(Self->StorageConfig->GetStateStorageConfig(), callback("explicit_state_storage_config"));
                enrich(Self->StorageConfig->GetStateStorageBoardConfig(), callback("explicit_state_storage_board_config"));
                enrich(Self->StorageConfig->GetSchemeBoardConfig(), callback("explicit_scheme_board_config"));
            }
            if (addV1) {
                auto equals = google::protobuf::util::MessageDifferencer::Equals;
                auto& sc = Self->StorageConfig;
                auto& ssConfig = sc->GetStateStorageConfig();
                auto& ssbConfig = sc->GetStateStorageBoardConfig();
                auto& sbConfig = sc->GetSchemeBoardConfig();
                if (equals(ssConfig, ssbConfig) && equals(ssConfig, sbConfig)) {
                    NKikimrConfig::TDomainsConfig domains;
                    domains.CopyFrom(AppData()->DomainsConfig);
                    domains.ClearStateStorage();
                    domains.AddStateStorage()->CopyFrom(ssConfig);
                    for (int i = 0, count = domains.DomainSize(); i < count; ++i) {
                        auto *dom = domains.MutableDomain(i);
                        dom->ClearExplicitCoordinators();
                        dom->ClearExplicitAllocators();
                        dom->ClearExplicitMediators();
                    }
                    enrich(domains, replace("domains_config"));
                } else {
                    throw TExError() << "State storage, state storage board and scheme board configs are not equal";
                }
            }
        });
    }

    void TInvokeRequestHandlerActor::ReplaceStorageConfig(const TQuery::TReplaceStorageConfig& request) {
        RunCommonChecks();

        if (!Self->ConfigCommittedToConsole && Self->SelfManagementEnabled) {
            throw TExRace() << "Previous config has not been committed to Console yet";
        }

        // extract YAML files provided by the user
        NewYaml = request.HasYAML() ? std::make_optional(request.GetYAML()) : std::nullopt;
        NewStorageYaml = request.HasStorageYAML() ? std::make_optional(request.GetStorageYAML()) : std::nullopt;

        // check if we are really changing something?
        if (NewYaml == Self->MainConfigYaml) {
            NewYaml.reset();
        }
        if (NewStorageYaml == Self->StorageConfigYaml) {
            NewStorageYaml.reset();
        }
        if (!NewYaml && !NewStorageYaml) {
            if (request.HasSwitchDedicatedStorageSection()) {
                throw TExError() << "Switching dedicated storage section mode without providing any new config";
            } else { // finish this request prematurely: no configs are actually changed
                return Finish(TResult::OK, std::nullopt);
            }
        }

        // start deriving a config from current one
        TString state;
        NKikimrBlobStorage::TStorageConfig config(*Self->StorageConfig);

        try {
            auto load = [&](const TString& yaml, ui64& version, const char *expectedKind) {
                state = TStringBuilder() << "loading " << expectedKind << " YAML";
                NJson::TJsonValue json = NYaml::Yaml2Json(YAML::Load(yaml), true);

                state = TStringBuilder() << "extracting " << expectedKind << " metadata";
                if (!json.Has("metadata") || !json["metadata"].IsMap()) {
                    throw yexception() << "no metadata section";
                }
                auto& metadata = json["metadata"];
                NYaml::ValidateMetadata(metadata);
                if (!metadata.Has("kind") || metadata["kind"] != expectedKind) {
                    throw yexception() << "missing or invalid kind provided";
                }
                version = metadata["version"].GetUIntegerRobust();
                if (metadata.Has("cluster") && metadata["cluster"] != AppData()->ClusterName) {
                    throw yexception() << "cluster name mismatch: provided# " << metadata["cluster"]
                        << " expected# " << AppData()->ClusterName;
                }

                state = TStringBuilder() << "validating " << expectedKind << " config section";
                if (!json.Has("config") || !json["config"].IsMap()) {
                    throw yexception() << "missing config section";
                }

                return json;
            };

            NJson::TJsonValue main;
            NJson::TJsonValue storage;
            const NJson::TJsonValue *effective = nullptr;

            if (NewStorageYaml) {
                storage = load(*NewStorageYaml, StorageYamlVersion.emplace(), "StorageConfig");
                config.SetExpectedStorageYamlVersion(*StorageYamlVersion + 1);
                effective = &storage;
            }

            if (NewYaml) {
                main = load(*NewYaml, MainYamlVersion.emplace(), "MainConfig");
                if (!effective && !request.GetDedicatedStorageSectionConfigMode()) {
                    // we will parse main config as distconf's one, as the user is not expecting us to have two
                    // separate configs; we'll check if this is correct later
                    effective = &main;
                }
            }

            if (effective) {
                state = "parsing final config";

                NKikimrConfig::TAppConfig appConfig;
                NYaml::Parse(*effective, NYaml::GetJsonToProtoConfig(), appConfig, true);

                if (TString errorReason; !DeriveStorageConfig(appConfig, &config, &errorReason)) {
                    throw TExError() << "Error while deriving StorageConfig: " << errorReason;
                }
            }
        } catch (const TExError& ex) {
            throw ex;
        } catch (const std::exception& ex) {
            throw TExError() << "Exception while " << state << ": " << ex.what();
        }

        // advance the config generation (may be necessary for validation purposes)
        config.SetGeneration(config.GetGeneration() + 1);

        // make it proposed one
        ProposedStorageConfig = std::move(config);

        // check if we are enabling distconf by this operation and handle it accordingly
        if (!Self->SelfManagementEnabled && ProposedStorageConfig.GetSelfManagementConfig().GetEnabled()) {
            ControllerOp = EControllerOp::ENABLE_DISTCONF;
            TryEnableDistconf(); // collect quorum of configs first to see if we have to do rolling restart of the cluster
        } else {
            if (Self->SelfManagementEnabled && !ProposedStorageConfig.GetSelfManagementConfig().GetEnabled()) {
                ControllerOp = EControllerOp::DISABLE_DISTCONF;
            } else {
                ControllerOp = EControllerOp::OTHER;
            }
            ConnectToController();
        }
    }

    void TInvokeRequestHandlerActor::ReplaceStorageConfigResume(const std::optional<TString>& storageConfigYaml, ui64 expectedMainYamlVersion,
            ui64 expectedStorageYamlVersion, bool enablingDistconf) {
        auto *op = std::get_if<TInvokeExternalOperation>(&Query);
        Y_ABORT_UNLESS(op);
        const auto& request = op->Command.GetReplaceStorageConfig();

        auto switchDedicatedStorageSection = request.HasSwitchDedicatedStorageSection()
            ? std::make_optional(request.GetSwitchDedicatedStorageSection())
            : std::nullopt;

        const bool targetDedicatedStorageSection = switchDedicatedStorageSection.value_or(storageConfigYaml.has_value());

        if (switchDedicatedStorageSection) {
            // check that configs are explicitly defined when we are switching dual-config mode
            if (!NewYaml) {
                throw TExError() << "Main config must be specified when switching dedicated storage section mode";
            } else if (*switchDedicatedStorageSection && !NewStorageYaml) {
                throw TExError() << "Storage config must be specified when turning on dedicated storage section mode";
            }
        }

        if (request.GetDedicatedStorageSectionConfigMode() != targetDedicatedStorageSection) {
            throw TExError() << "DedicatedStorageSectionConfigMode does not match target state";
        } else if (NewStorageYaml && !targetDedicatedStorageSection) {
            // we are going to end up in single-config mode, but explicit storage yaml is provided
            throw TExError() << "Unexpected dedicated storage config section in request";
        } else if (switchDedicatedStorageSection && *switchDedicatedStorageSection == storageConfigYaml.has_value()) {
            // this enable/disable command does not change the state
            throw TExError() << "Dedicated storage config section is already in requested state";
        }

        if (StorageYamlVersion && *StorageYamlVersion != expectedStorageYamlVersion) {
            throw TExError() << "Storage config version must be increasing by one"
                << " new version# " << *StorageYamlVersion
                << " expected version# " << expectedStorageYamlVersion;
        }

        if (MainYamlVersion && *MainYamlVersion != expectedMainYamlVersion) {
            throw TExError() << "Main config version must be increasing by one"
                << " new version# " << *MainYamlVersion
                << " expected version# " << expectedMainYamlVersion;
        }

        if (auto error = ValidateConfig(*Self->StorageConfig)) {
            throw TExError() << "ReplaceStorageConfig current config validation failed: " << *error;
        } else if (auto error = ValidateConfigUpdate(*Self->StorageConfig, ProposedStorageConfig)) {
            throw TExError() << "ReplaceStorageConfig config validation failed: " << *error;
        }

        // update main config yaml in the StorageConfig
        if (NewYaml) {
            if (const auto& error = UpdateConfigComposite(ProposedStorageConfig, *NewYaml, std::nullopt)) {
                throw TExError() << "Failed to update config yaml: " << *error;
            }
        }

        // do the same thing for storage config yaml
        if (NewStorageYaml) {
            TString s;
            if (TStringOutput output(s); true) {
                TZstdCompress zstd(&output);
                zstd << *NewStorageYaml;
            }
            ProposedStorageConfig.SetCompressedStorageYaml(s);
        } else if (!targetDedicatedStorageSection) {
            ProposedStorageConfig.ClearCompressedStorageYaml();
        }

        if (request.GetSkipConsoleValidation() || !NewYaml) {
            StartProposition(&ProposedStorageConfig);
        } else if (!Self->EnqueueConsoleConfigValidation(SelfId(), enablingDistconf, *NewYaml)) {
            throw TExRace() << "Console pipe is not available";
        }
    }

    void TInvokeRequestHandlerActor::TryEnableDistconf() {
        TEvScatter task;
        task.MutableCollectConfigs();
        IssueScatterTask(std::move(task), [this](TEvGather *res) {
            Y_ABORT_UNLESS(Self->StorageConfig); // it can't just disappear
            Y_ABORT_UNLESS(!Self->CurrentProposition);

            if (!res->HasCollectConfigs()) {
                throw TExError() << "Incorrect CollectConfigs response";
            } else if (auto r = Self->ProcessCollectConfigs(res->MutableCollectConfigs(), std::nullopt); r.ErrorReason) {
                throw TExError() << *r.ErrorReason;
            } else if (r.ConfigToPropose) {
                throw TExError() << "Unexpected config proposition";
            } else if (r.IsDistconfDisabledQuorum) {
                // distconf is disabled on the majority of nodes; we have just to replace configs
                // and then to restart these nodes in order to enable it in future
                Finish(TResult::CONTINUE_BSC, "proceed with BSC", [&](TResult *record) {
                    record->MutableReplaceStorageConfig()->SetAllowEnablingDistconf(true);
                });
            } else {
                ConnectToController();
            }
        });
    }

    void TInvokeRequestHandlerActor::ConnectToController() {
        ControllerPipeId = Register(NTabletPipe::CreateClient(SelfId(), MakeBSControllerID(),
            NTabletPipe::TClientRetryPolicy::WithRetries()));
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerConfigRequest>();
        ev->Record.MutableRequest()->AddCommand()->MutableGetInterfaceVersion();
        NTabletPipe::SendData(SelfId(), ControllerPipeId, ev.release());
    }

    void TInvokeRequestHandlerActor::Handle(TEvTabletPipe::TEvClientConnected::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BS_NODE, NWDC65, "received TEvClientConnected", (SelfId, SelfId()), (Status, msg.Status),
            (ClientId, msg.ClientId), (ServerId, msg.ServerId));

        if (msg.Status != NKikimrProto::OK) {
            ControllerPipeId = {};
            throw TExError() << "Failed to connect to BSC with " << msg.Status;
        }
    }

    void TInvokeRequestHandlerActor::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BS_NODE, NWDC79, "received TEvClientDestroyed", (SelfId, SelfId()),
            (ClientId, msg.ClientId), (ServerId, msg.ServerId));

        ControllerPipeId = {};
        throw TExError() << "Pipe to BSC disconnected";
    }

    void TInvokeRequestHandlerActor::Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr ev) {
        const auto& response = ev->Get()->Record.GetResponse();
        STLOG(PRI_DEBUG, BS_NODE, NWDC80, "received TEvControllerConfigResponse", (SelfId, SelfId()),
            (Response, response));
        if (response.StatusSize() != 1 || response.GetStatus(0).GetInterfaceVersion() < BSC_INTERFACE_DISTCONF_CONTROL) {
            throw TExError() << "BS_CONTROLLER is way too old to process this query";
        }

        auto request = std::make_unique<TEvBlobStorage::TEvControllerDistconfRequest>();
        auto& record = request->Record;
        auto *op = std::get_if<TInvokeExternalOperation>(&Query);
        Y_ABORT_UNLESS(op);
        const auto& replaceStorageConfig = op->Command.GetReplaceStorageConfig();

        // provide the full main config to the recipient (in case when user changes it, or when we are managing it)
        if (NewYaml) {
            record.SetCompressedMainConfig(NYamlConfig::CompressYamlString(*NewYaml));
        } else if (Self->SelfManagementEnabled) {
            record.SetCompressedMainConfig(NYamlConfig::CompressYamlString(Self->MainConfigYaml));
        }

        // do the same thing to the storage config
        if (NewStorageYaml) {
            record.SetCompressedStorageConfig(NYamlConfig::CompressYamlString(*NewStorageYaml));
        } else if (Self->SelfManagementEnabled && Self->StorageConfigYaml) {
            record.SetCompressedStorageConfig(NYamlConfig::CompressYamlString(*Self->StorageConfigYaml));
        }

        record.SetDedicatedConfigMode(replaceStorageConfig.GetDedicatedStorageSectionConfigMode());

        // fill in operation and operation-dependent fields
        switch (ControllerOp) {
            case EControllerOp::ENABLE_DISTCONF:
                record.SetOperation(NKikimrBlobStorage::TEvControllerDistconfRequest::EnableDistconf);
                break;

            case EControllerOp::DISABLE_DISTCONF:
                record.SetOperation(NKikimrBlobStorage::TEvControllerDistconfRequest::DisableDistconf);
                if (ProposedStorageConfig.HasExpectedStorageYamlVersion()) {
                    record.SetExpectedStorageConfigVersion(ProposedStorageConfig.GetExpectedStorageYamlVersion());
                    record.SetPeerName(replaceStorageConfig.GetPeerName());
                    record.SetUserToken(replaceStorageConfig.GetUserToken());
                }
                break;

            case EControllerOp::OTHER:
                record.SetOperation(NKikimrBlobStorage::TEvControllerDistconfRequest::ValidateConfig);
                break;

            case EControllerOp::UNSET:
                Y_DEBUG_ABORT();
        }

        NTabletPipe::SendData(SelfId(), ControllerPipeId, request.release());
    }

    void TInvokeRequestHandlerActor::Handle(TEvBlobStorage::TEvControllerDistconfResponse::TPtr ev) {
        auto& record = ev->Get()->Record;

        std::optional<TString> mainYaml;
        std::optional<TString> storageYaml;

        if (record.HasCompressedMainConfig()) {
            mainYaml = NYamlConfig::DecompressYamlString(record.GetCompressedMainConfig());
        }
        if (record.HasCompressedStorageConfig()) {
            storageYaml = NYamlConfig::DecompressYamlString(record.GetCompressedStorageConfig());
        }

        auto getRecord = [&] {
            if (mainYaml) {
                record.SetCompressedMainConfig(*mainYaml);
            }
            if (storageYaml) {
                record.SetCompressedStorageConfig(*storageYaml);
            }
            return record;
        };

        STLOG(PRI_DEBUG, BS_NODE, NWDC81, "received TEvControllerDistconfResponse", (SelfId, SelfId()),
            (Record, getRecord()));

        if (const auto& status = record.GetStatus(); status != NKikimrBlobStorage::TEvControllerDistconfResponse::OK) {
            throw TExError() << "Failed to interact with BSC to update configuration"
                << " Status# " << NKikimrBlobStorage::TEvControllerDistconfResponse::EStatus_Name(status)
                << " ErrorReason# " << record.GetErrorReason();
        }

        if (ControllerOp == EControllerOp::ENABLE_DISTCONF && !NewYaml) {
            // in case we are enabling distconf through dedicated storage section
            NewYaml = std::move(mainYaml);
        }

        switch (ControllerOp) {
            case EControllerOp::ENABLE_DISTCONF:
                ReplaceStorageConfigResume(storageYaml, record.GetExpectedMainConfigVersion(),
                    record.GetExpectedStorageConfigVersion(), true);
                break;

            case EControllerOp::DISABLE_DISTCONF:
            case EControllerOp::OTHER: {
                const ui64 expectedMainYamlVersion = Self->MainConfigYamlVersion
                    ? *Self->MainConfigYamlVersion + 1
                    : 0;
                ReplaceStorageConfigResume(Self->StorageConfigYaml, expectedMainYamlVersion,
                    Self->StorageConfig->GetExpectedStorageYamlVersion(), false);
                ProposedStorageConfig.ClearConfigComposite();
                ProposedStorageConfig.ClearCompressedStorageYaml();
                ProposedStorageConfig.ClearExpectedStorageYamlVersion();
                break;
            }

            case EControllerOp::UNSET:
                Y_DEBUG_ABORT();
        }
    }

    void TInvokeRequestHandlerActor::Handle(TEvBlobStorage::TEvControllerValidateConfigResponse::TPtr ev) {
        const auto& record = ev->Get()->Record;
        STLOG(PRI_DEBUG, BS_NODE, NWDC77, "received TEvControllerValidateConfigResponse", (SelfId, SelfId()),
            (InternalError, ev->Get()->InternalError), (Status, record.GetStatus()));

        if (ev->Get()->InternalError) {
            throw TExError() << "Failed to validate config through console: " << *ev->Get()->InternalError;
        }

        switch (record.GetStatus()) {
            case NKikimrBlobStorage::TEvControllerValidateConfigResponse::IdPipeServerMismatch:
                Self->DisconnectFromConsole();
                Self->ConnectToConsole();
                throw TExError() << "Console connection race detected: " << record.GetErrorReason();

            case NKikimrBlobStorage::TEvControllerValidateConfigResponse::ConfigNotValid:
                throw TExError() << "Console config validation failed: " << record.GetErrorReason();

            case NKikimrBlobStorage::TEvControllerValidateConfigResponse::ConfigIsValid:
                if (const auto& error = UpdateConfigComposite(ProposedStorageConfig, *NewYaml, record.GetYAML())) {
                    throw TExError() << "Failed to update config yaml: " << *error;
                }
                return StartProposition(&ProposedStorageConfig);
        }
    }

    void TInvokeRequestHandlerActor::BootstrapCluster(const TString& selfAssemblyUUID) {
        RunCommonChecks();

        if (Self->StorageConfig->GetGeneration()) {
            if (Self->StorageConfig->GetSelfAssemblyUUID() == selfAssemblyUUID) { // repeated command, it's ok
                return Finish(TResult::OK, std::nullopt);
            } else {
                throw TExError() << "Bootstrap on already bootstrapped cluster";
            }
        } else if (!selfAssemblyUUID) {
            throw TExError() << "SelfAssemblyUUID can't be empty";
        }

        // issue scatter task to collect configs and then bootstrap cluster with specified cluster UUID
        auto done = [this, selfAssemblyUUID = TString(selfAssemblyUUID)](TEvGather *res) {
            if (!res->HasCollectConfigs()) {
                throw TExError() << "Incorrect response to CollectConfigs";
            }

            Y_ABORT_UNLESS(Self->StorageConfig); // it can't just disappear
            Y_ABORT_UNLESS(!Self->CurrentProposition); // nobody couldn't possibly start proposing anything while we were busy

            if (Self->StorageConfig->GetGeneration()) {
                throw TExRace() << "Storage config generation regenerated while collecting configs";
            } else if (auto r = Self->ProcessCollectConfigs(res->MutableCollectConfigs(), selfAssemblyUUID); r.ErrorReason) {
                throw TExError() << *r.ErrorReason;
            } else if (r.ConfigToPropose) {
                CheckSyncersAfterCommit = r.CheckSyncersAfterCommit;
                StartProposition(&r.ConfigToPropose.value());
            } else { // no new proposition has been made
                Finish(TResult::OK, std::nullopt);
            }
        };

        TEvScatter task;
        task.MutableCollectConfigs();
        IssueScatterTask(std::move(task), std::move(done));
    }

} // NKikimr::NStorage
