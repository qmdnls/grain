#include <arrow/api.h>                 // pulls in table, result, etc.
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#include <iostream>


arrow::Result<int64_t> count_rows(const std::string& path) {
    ARROW_ASSIGN_OR_RAISE(auto infile, arrow::io::ReadableFile::Open(path));

    ARROW_ASSIGN_OR_RAISE(
        std::unique_ptr<parquet::arrow::FileReader> reader,
        parquet::arrow::OpenFile(infile, arrow::default_memory_pool())
    );  // arrow 20+
    
    std::shared_ptr<arrow::Table> table;
    ARROW_RETURN_NOT_OK(reader->ReadTable(&table));

    return table->num_rows();
}

int main(int argc, char** argv) {
    if (argc<2) {
        std::cerr<<"usage: "<<argv[0]<<" file.parquet\n";return 1;
    }
    
    auto maybe_rows = count_rows(argv[1]);
    
    if (!maybe_rows.ok()) {
        std::cerr << maybe_rows.status() << "\n";
        return 1;
    }

    std::cout << "rows: " << maybe_rows.ValueOrDie() << "\n";
    return 0;
}
