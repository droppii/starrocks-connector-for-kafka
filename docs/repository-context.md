# Ngữ cảnh repository — starrocks-connector-for-kafka

Plugin Kafka Connect đưa record vào StarRocks qua sink task.

## 1. Trách nhiệm và luồng xử lý

Connector tạo task; converter/transform chuẩn hóa record; schema manager có thể bổ sung cột khi bật evolution trước khi nạp dữ liệu.

Giữ offset/ack, retry, idempotency và chuyển đổi decimal/thời gian. Schema evolution là opt-in, chỉ thêm cột và không tạo bảng; không tự bật cho production. Kiểm thử tombstone/CDC và lỗi một phần của batch.

## 2. Điểm bắt đầu trong mã

- [src/main/java/com/droppii/connector/kafka](../src/main/java/com/droppii/connector/kafka).
- [src/test/java/com/droppii/connector/kafka](../src/test/java/com/droppii/connector/kafka).
- [pom.xml](../pom.xml).
- [README.md](../README.md).

Đọc hướng dẫn tại thư mục con trước khi sửa module tương ứng. Đối chiếu hợp đồng và nơi gọi khi thay đổi API, dữ liệu hoặc thư viện dùng chung.

## 3. Toolchain và kiểm chứng

Chuẩn bị môi trường theo [pom.xml](../pom.xml). Phiên bản runtime của image và dependency phát triển cần được đối chiếu khi khác nhau.

| Mục đích | Thư mục chạy | Lệnh | Điều kiện và tác động |
| --- | --- | --- | --- |
| Kiểm thử và đóng gói | `.` | `mvn test` | JDK theo pom.xml, Maven tải dependency; xem fixture của test trước khi kết nối Kafka/StarRocks. |

Điểm vào bộ kiểm thử: [src/test/java/com/droppii/connector/kafka/StarRocksSinkConnectorConfigTest.java](../src/test/java/com/droppii/connector/kafka/StarRocksSinkConnectorConfigTest.java), [src/test/java/com/droppii/connector/kafka/StarRocksSinkTaskTest.java](../src/test/java/com/droppii/connector/kafka/StarRocksSinkTaskTest.java), [src/test/java/com/droppii/connector/kafka/json/JsonConverterTest.java](../src/test/java/com/droppii/connector/kafka/json/JsonConverterTest.java), [src/test/java/com/droppii/connector/kafka/json/TemporalTypeFormatsTest.java](../src/test/java/com/droppii/connector/kafka/json/TemporalTypeFormatsTest.java). Đọc setup/fixture trước khi chạy rộng hơn; những đường dẫn này không biểu thị mức bao phủ của toàn hệ thống.

Ghi kết quả và phạm vi thực sự đã chạy trong PR. Không ghi build, lint hoặc test thành công khi mới đọc cấu hình hay khi script không thực thi assertion.

## 4. Cấu hình và phát hành

Đường tự động hóa được xác định trong các workflow sau. Điều kiện branch/tag/path quyết định lúc chạy; bước phát hành cần được đọc cùng quyền và môi trường của job.

| Workflow | Điều kiện kích hoạt |
| --- | --- |
| [.github/workflows/review-pipeline.yml](../.github/workflows/review-pipeline.yml) | pull_request_target; branches=main; types=opened, synchronize |

Dùng credential được cấp cho đúng môi trường. Không lưu secret hoặc dữ liệu cá nhân vào mã, tài liệu, ảnh chụp hay log. Migration, seed, publish, gửi thông báo và rollout là thao tác có tác động; chỉ thực hiện trong phạm vi công việc và môi trường đã được giao. Phiên bản đang phục vụ cần được xác định từ artifact/deployment, không chỉ từ default branch.

## 5. Tiêu chuẩn chung và duy trì

Áp dụng [Engineering Standards](https://github.com/droppii/engineering-knowledge/tree/docs/repository-context-pilot/handbook/standards/standards.md), [Principles](https://github.com/droppii/engineering-knowledge/tree/docs/repository-context-pilot/handbook/principles/principles.md) và [AI Context](https://github.com/droppii/engineering-knowledge/tree/docs/repository-context-pilot/handbook/ai-context/README.md). Cấu trúc đang có giải thích cách tìm mã; những phần chưa theo tiêu chuẩn cần được cải thiện khi sửa, không trở thành ngoại lệ mặc định.

Cập nhật tài liệu này trong cùng thay đổi khi module, toolchain, public contract, pipeline hoặc điều kiện kiểm thử thay đổi. Quyết định đặc thù cần được ghi ở repository; kiến thức và tiêu chuẩn dùng chung được duy trì tại Engineering Knowledge.
