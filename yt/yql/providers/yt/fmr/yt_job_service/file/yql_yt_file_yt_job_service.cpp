#include "yql_yt_file_yt_job_service.h"
#include <library/cpp/yson/parser.h>
#include <util/stream/file.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file_text_yson.h>
#include <yt/yql/providers/yt/lib/yson_helpers/yson_helpers.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NFmr {

namespace {

class TFileYtTableWriter: public NYT::TRawTableWriter {
public:
    TFileYtTableWriter(const TString& filePath): FilePath_(filePath) {}

    void NotifyRowEnd() override {
    }

private:
    void DoWrite(const void* buf, size_t len) override {
        Buffer_.Append(static_cast<const char*>(buf), len);
    }

    void DoFlush() override {
        TMemoryInput input(Buffer_.data(), Buffer_.size());
        TFile outputFile(FilePath_, OpenAlways | WrOnly | ForAppend);
        TFileOutput outputFileStream(outputFile);
        TDoubleHighPrecisionYsonWriter writer(&outputFileStream, ::NYson::EYsonType::ListFragment);
        NYson::TYsonParser parser(&writer, &input, ::NYson::EYsonType::ListFragment);
        parser.Parse();
        Buffer_.Clear();
    }

    TString FilePath_;
    TBuffer Buffer_;
};

class TFileYtJobService: public NYql::NFmr::IYtJobService {
public:

NYT::TRawTableReaderPtr MakeReader(
    const TYtTableRef& ytTablePart,
    const TClusterConnection& /*clusterConnection*/,
    const TYtReaderSettings& /*readerSettings*/
) override {
        TMaybe<TString> filePath = ytTablePart.FilePath;
        YQL_ENSURE(filePath.Defined(), "File path should be set for file yt reader");

        NFile::TColumnsInfo columnsInfo;
        auto ytPath = ytTablePart.RichPath;
        if (!ytPath.Columns_.Empty()) {
            TVector<TString> columns(ytPath.Columns_->Parts_.begin(), ytPath.Columns_->Parts_.end());
            columnsInfo.Columns = NYT::TSortColumns(columns);
        }

        auto textYsonInputs = NFile::MakeTextYsonInputs({{*filePath, columnsInfo}}, false);
        return textYsonInputs[0];
    }

    NYT::TRawTableWriterPtr MakeWriter(
        const TYtTableRef& ytTable,
        const TClusterConnection& /*clusterConnection*/,
        const TYtWriterSettings& /*writerSettings*/
    ) override {
        YQL_ENSURE(ytTable.FilePath);
        return MakeIntrusive<TFileYtTableWriter>(*ytTable.FilePath);
    }
};

} // namespace

IYtJobService::TPtr MakeFileYtJobSerivce() {
    return MakeIntrusive<TFileYtJobService>();
}

} // namespace NYql::NFmr
