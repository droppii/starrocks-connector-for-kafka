# Profile thực thi — starrocks-connector-for-kafka

Profile được quản lý cùng mã nguồn và dùng lại trong checkout/worktree của mỗi công việc. Nền tảng agent quản lý nơi làm việc và quyền truy cập. Cấu hình này không tạo agent, nhánh Git, worktree hoặc môi trường cloud.

## Chọn và sử dụng

Từ gốc repo, dùng Python 3.9 trở lên:

```sh
python3 .engineering/run.py list
python3 .engineering/run.py check
python3 .engineering/run.py run platform-inputs --task WORK-123
```

Thay `WORK-123` bằng mã công việc. `check` kiểm hợp đồng profile và tham chiếu nguồn, không chạy test ứng dụng. `run` chạy đúng profile đã chọn và lưu kết quả riêng tại `.engineering/runs/<run-id>/result.json`. Với source chưa materialize trong sparse checkout, kiểm tra nguồn đọc từ Git HEAD; lệnh công cụ cần file/dependency hiện diện trên đĩa trước khi chạy. Thay đổi chưa commit ở file hiện diện được đọc trực tiếp.

| Profile | Phạm vi | Bằng chứng |
| --- | --- | --- |
| `platform-inputs` | Đối chiếu đầu vào cấu hình nền tảng | source-validation |
| `plugin-package` | Đóng gói plugin Java | build |

## Chuẩn bị module

[profiles.json](profiles.json) giữ thư mục module, manifest, nguồn toolchain, lockfile và bước chuẩn bị tương ứng. Đọc cùng [ngữ cảnh repository](../docs/repository-context.md). Dùng phiên bản của repo/module; không lấy phiên bản trên máy hoặc template mới thay thế mặc định. Khi khai báo toolchain chưa đủ hoặc có mâu thuẫn giữa manifest và Dockerfile, xác định đường chạy cần tái hiện rồi ghi lựa chọn trong hồ sơ công việc.

Cài dependency trong môi trường của công việc. Lệnh setup không tự được thực hiện khi chạy profile. Với JavaScript, giữ lockfile và tắt lifecycle script ở lần cài đầu; đọc `postinstall`/`prepare` để bổ sung đúng bước sinh mã hoặc patch cần thiết. Với Python, dùng virtual environment riêng; requirements chưa khóa toàn bộ dependency cần ghi lại các phiên bản thực tế. Các module không có lockfile phù hợp chưa có lệnh cài tái tạo, không tự xóa hoặc tạo lại lock để vượt lỗi.

## Ý nghĩa kết quả và quyền thực thi

`source-validation` chỉ phân tích Python AST, JSON nghiêm ngặt hoặc XML khi phù hợp; file khác được nhận diện bằng hash. Nó không import ứng dụng, gọi API, kết nối database hoặc thực thi notebook. `static-analysis` chạy công cụ kiểm mã/kiểu sau khi đã chuẩn bị dependency; không chứng minh hành vi nghiệp vụ. Build artifact cũng không thay test. Những luồng cần API, broker, database, thiết bị hoặc model phải có dữ liệu thử, cấu hình và bằng chứng bổ sung trước khi kết luận hoàn tất.

Khi cần unit test, chọn đúng suite và kiểm số ca thực thi, test bị bỏ qua, exit code và report; không dùng watch mode, `passWithNoTests` hoặc script placeholder làm bằng chứng thành công. Các profile kiểm thử hành vi chuyên biệt được dẫn trong ngữ cảnh repo khi đã có. Các lệnh phát hành, ký native, migration, seed, cloud apply và gọi dịch vụ thật không nằm trong bước kiểm mặc định.

Lệnh công cụ chạy trong workspace hiện có, chỉ nhận một số biến môi trường công cụ và HOME tạm; cơ chế này không cách ly filesystem/network của agent. Nền tảng gọi lệnh phải cấp đúng quyền và môi trường; không đặt secret hoặc cấu hình dịch vụ thật trong môi trường thử. Cấu hình có thể được công cụ nạp từ repo nên cần đọc trước khi chạy. Kết quả ghi revision, hash profile/đầu vào, loại bằng chứng và giới hạn; log không được đưa vào Git và cần rà dữ liệu nhạy cảm trước khi chia sẻ. Khi timeout hoặc ngắt, script dừng nhóm tiến trình do nó khởi tạo và giữ kết quả để bàn giao.

## Duy trì

Cập nhật profile cùng thay đổi manifest, đường dẫn hoặc cách kiểm chứng. Script `run.py` dùng nguồn chung từ Engineering Knowledge (`handbook/ai-context/tools/repository_profile.py`); đồng bộ có chủ đích và kiểm lại sau khi sửa, không tải mã thực thi mới tự động từ mạng. Kết quả cụ thể của từng lần chạy nằm trong hồ sơ công việc/PR, không ghi trạng thái đã kiểm toàn hệ thống vào cấu hình dùng lại.
