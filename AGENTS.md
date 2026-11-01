# Hướng dẫn làm việc — starrocks-connector-for-kafka

## Ngữ cảnh Engineering

Plugin Kafka Connect đưa record vào StarRocks qua sink task.

Đọc [ngữ cảnh repository](docs/repository-context.md) để chọn đúng module, toolchain, kiểm tra và đường phát hành. Đọc hướng dẫn thư mục con khi đi vào phạm vi tương ứng.

Chuẩn dùng chung: [AI Context](https://github.com/droppii/engineering-knowledge/tree/docs/repository-context-pilot/handbook/ai-context/README.md) và [Standards](https://github.com/droppii/engineering-knowledge/tree/docs/repository-context-pilot/handbook/standards/standards.md). Ghi revision đã dùng và kết quả kiểm chứng trong PR; cập nhật ngữ cảnh cùng mã.

Giữ offset/ack, retry, idempotency và chuyển đổi decimal/thời gian. Schema evolution là opt-in, chỉ thêm cột và không tạo bảng; không tự bật cho production. Kiểm thử tombstone/CDC và lỗi một phần của batch.

## Profile thực thi

Đọc [.engineering/README.md](.engineering/README.md) để chọn profile theo module và nhu cầu kiểm chứng. Dùng checkout/worktree do công cụ agent quản lý theo công việc; phân biệt kiểm nguồn, kiểm mã và kiểm thử hành vi.
