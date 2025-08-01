LIBRARY()

SRCS(
    kqp_proxy_service.cpp
    kqp_proxy_databases_cache.cpp
    kqp_proxy_peer_stats_calculator.cpp
    kqp_script_execution_retries.cpp
    kqp_script_executions.cpp
    kqp_session_info.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/http
    library/cpp/protobuf/interop
    library/cpp/protobuf/json
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/cms/console
    ydb/core/kqp/common
    ydb/core/kqp/common/events
    ydb/core/kqp/counters
    ydb/core/kqp/gateway/behaviour/resource_pool_classifier
    ydb/core/kqp/proxy_service/proto
    ydb/core/kqp/run_script_actor
    ydb/core/kqp/workload_service
    ydb/core/mind
    ydb/core/protos
    ydb/core/tx/tx_proxy
    ydb/core/tx/scheme_cache
    ydb/core/tx/schemeshard
    ydb/core/mon
    ydb/library/query_actor
    ydb/library/table_creator
    ydb/library/yql/providers/common/http_gateway
    yql/essentials/providers/common/proto
    ydb/library/yql/providers/s3/actors_factory
    yql/essentials/public/issue
    ydb/library/yql/dq/actors/spilling
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/library/operation_id
    ydb/public/lib/scheme_types
    ydb/public/sdk/cpp/src/client/params
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
