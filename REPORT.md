# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Nguyễn Văn Bảo |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602862 |
| GitHub | Baor-AI |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-06667 |

> Ghi chú về trip: MSSV `2A202602862` có chữ `A` nên `int(MSSV)` trong lệnh của GUIDE báo lỗi. Tôi bỏ ký tự không phải số → `2202602862`, `2202602862 % 26 = 18`, phần tử thứ 18 của `list-trips` là `trip-06667`.

## Kết quả cuối

- `dataset_version` của bạn: `59cc937c0594`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: 664 / 171 / 184 (tổng 1019 khung; 18 / 4 / 4 trip)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Khác biệt thấy trong 2 dòng đầu của `bronze.raw_telemetry_a` (3021 dòng) và `raw_telemetry_b` (3234 dòng):
1. **Tên cột**: A dùng `ts_utc, speed_mps, heading_deg, lat, lon, alt_m, acc_x, gas, brake, steer`; B dùng `timestamp_ms, spd_kmh, hdg, latitude, longitude, altitude, ax, throttle_pct, brake_pct, steering`.
2. **Đơn vị tốc độ**: A là m/s (`1.213`), B là km/h (`55.11`).
3. **Kiểu thời gian**: A là chuỗi ISO UTC (`2025-10-07T08:00:00.014Z`), B là epoch milli giây (`1759933801509`).
4. **Bàn đạp**: A là tỉ lệ 0..1 (`gas 0.028`), B là phần trăm (`throttle_pct 2.0`).
5. **Đồng hồ**: `bronze.video_files` ghi `telemetry_clock_offset_ms` = 0 cho drive_a nhưng **1500** cho drive_b (đồng hồ thiết bị B chạy nhanh 1,5 s so với video).

Giữ mỗi nguồn một bảng và để mọi cột là chữ vì bronze phải là **bản sao trung thực, chỉ thêm** của dữ liệu thô (slide 14): nếu ép kiểu/gộp ngay thì một ô trống, một giá trị lạ hay một lần đổi schema của nguồn sẽ làm hỏng lúc nạp hoặc bị biến đổi im lặng, và ta mất bản gốc để xử lý lại. Việc chuẩn hoá để cho silver (dbt), nơi có thể sửa logic rồi chạy lại từ bronze. Chạy ingest lần 2 báo `already present` (nhận diện bằng sha256), và `cli history bronze.raw_telemetry_a` chỉ có **1 commit** (version 0, `rows_added: 3021`).

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

`stg_telemetry_b.sql` đổi tên cột về schema của A, chia `spd_kmh / 3.6` để ra m/s, chia pedal % cho 100, và tính `ts = to_timestamp((timestamp_ms - offset_ms) / 1000)` với `offset_ms` = 1500 lấy từ `bronze.video_files` (khai báo trong `sources.json`). A chỉ cần ép kiểu (`ts_utc` → timestamptz).

Bằng chứng: sau chuẩn hoá, `bat_dau` của drive_b là `2025-10-08 14:30:00.009`, khớp giờ bắt đầu video `14:30:00` (lệch 9 ms; nếu chưa trừ sẽ lệch 1,5 s); tốc độ tối đa 71 km/h (A) và 100 km/h (B), cùng đơn vị và hợp lý. Verify: `drive B clock offset removed` PASS.

| drive_id | duplicate | speed_missing | speed_out_of_range | sạch + cách ly = nhận |
|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 2968 + 53 = 3021 |
| drive_b | 20 | 30 | 3 | 3181 + 53 = 3234 |

Tổng `silver.telemetry` = 6149 dòng, `silver.telemetry_rejects` = 106 dòng. Dòng lỗi vào `rejects` kèm `reject_reason` thay vì bị xoá để **đối soát được** (sạch + cách ly = nhận, không mất dòng nào im lặng), để điều tra nguồn gây lỗi (ví dụ cảm biến tốc độ hay rơi giá trị), và để xử lý lại nếu luật làm sạch sai hoặc được sửa.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` (khoảng trống > 1000 ms giữa hai mẫu) đo **tình trạng thật của cảm biến** (mất tín hiệu): dữ liệu vẫn đúng, chỉ thưa hơn, và join ASOF đã có `sync_ok` để loại khung không có telemetry gần. Dừng pipeline vì nó thì mỗi lần mất sóng là hỏng cả lô, nên chỉ `warn` (bước 2 cho `PASS=6 WARN=1`). Còn `in_range` (0..70 m/s) kiểm tra **tính hợp lệ của dữ liệu**: tốc độ 1000 m/s là vô lý, nếu lọt vào gold sẽ làm hỏng nhãn huấn luyện, nên phải `error` để chặn các task phía sau.

`break_gate.json`: nới `max_speed_mps` lên **1000** → `dbt test` cho `PASS=5 WARN=1 ERROR=1`; test **`in_range_silver_telemetry_speed_mps__70__0`** bắt được **6 dòng** (đúng 3 + 3 dòng `speed_out_of_range` của hai drive giờ lọt vào silver). Khôi phục luật: `PASS=6 WARN=1 ERROR=0`. Điều này cho thấy cổng chất lượng là **lớp phòng thủ độc lập với luật làm sạch**: dù ai đó nới/sai luật transform, test vẫn khẳng định bất biến của dữ liệu và dừng DAG trước gold.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

`idempotency.json` (trip `trip-00003`, 60 khung): trước **1246** dòng, sau hai lần chạy lại vẫn **1246**; dấu vân tay `6c7480dfde7e` trước = sau (`unchanged: true`). Nếu ghi kiểu `append`, sau hai lần chạy trip đó có **120** dòng thay vì 60, tức nhân đôi.

Delta `overwrite` với predicate `trip_id = ...` là **một commit nguyên tử** vào transaction log: file mới được ghi xong rồi mới có một commit vừa đánh dấu file cũ của partition là removed vừa thêm file mới. Nếu tiến trình chết giữa chừng thì commit không xảy ra, người đọc vẫn thấy phiên bản cũ đầy đủ. Còn "xoá rồi ghi lại" là hai thao tác: sập sau khi xoá thì partition **biến mất** (mất dữ liệu), và người đọc ở giữa thấy bảng thiếu. Overwrite theo partition cũng chỉ đụng đúng trip đó, các trip khác giữ nguyên, nên backfill/chạy lại một trip là an toàn (lịch sử `silver.frames` lên version 27 = 26 lần ghi trip + 2 lần chạy lại, nhưng số dòng không đổi).

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung **đen** (độ sáng trung bình < 12, `dark_luma`) là lỗi camera/truyền hình ảnh, không có thông tin → **loại** (A: 6, B: 4). Khung **tối** (< 40, `dark_soft`) là điều kiện thật (đường hầm, bóng râm, đêm) → **gắn cờ nhưng giữ lại** (A: 4, B: 61). Tôi đồng ý: mô hình xe tự lái phải học cả cảnh thiếu sáng, bỏ chúng sẽ làm dataset lệch về cảnh ban ngày.

Tôi giữ ngưỡng **2**: ngưỡng 0 chỉ bắt khung giống hệt bit (A 61) nên bỏ sót phần lớn 6 giây camera đứng hình (nhiễu nén làm vài bit khác); ngưỡng 6 coi 363/602 khung của A (60%) là trùng, tức xoá cả khung xe đang chạy chậm có nội dung mới. Với 2, A mất 149 (xe chậm + đứng hình, verify bắt 11/12 khung đứng hình) còn B (chạy nhanh hơn, ~69–90 km/h) chỉ mất 53. Ngưỡng phụ thuộc tốc độ: xe càng chậm, hai khung cách 0,5 s càng giống, nên cùng một ngưỡng sẽ xoá nhiều khung hơn; với dữ liệu nhiều đoạn đứng yên/kẹt xe có thể cần ngưỡng thấp hơn hoặc ngưỡng theo tốc độ (dùng `speed_mps` từ telemetry).

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

`leakage.json`: với 1019 khung giữ lại và 26 trip, chia **theo khung hình** làm **26/26 trip** bị chia cắt, và cả **26** trip có khung ở **cả train lẫn test**. Chia **theo trip** (cách pipeline dùng): **0** trip bị chia cắt (verify: `no trip is in two splits` PASS).

Các khung liên tiếp trong một trip chỉ cách nhau 0,5 s nên gần như giống hệt nhau (cùng đường, cùng ánh sáng, cùng xe phía trước). Khi chia theo khung hình, mô hình đã "thấy" gần như chính ảnh test lúc huấn luyện, nên điểm test đo khả năng **nhớ** cảnh chứ không phải **khái quát** sang cảnh mới; khi triển khai trên con đường chưa gặp, điểm thật sẽ thấp hơn nhiều. Chia theo trip (cảnh) đảm bảo test là cảnh hoàn toàn mới.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` = 12 ký tự đầu của sha256 trên danh sách `frame_id|split` (`pipeline/publish.py`). Nó là băm nội dung vì phiên bản phải **đại diện cho chính dữ liệu**: cùng nội dung → cùng version (chạy lại không sinh version "mới" giả), khác một khung hay một split → version khác. Ngày giờ hay số đếm thay đổi mỗi lần chạy dù dữ liệu y hệt, và không cho biết hai dataset có giống nhau không.

Hai lần chạy liên tiếp (`step 4` rồi `cli export-dataset`) đều cho **`59cc937c0594`** (cùng 664/171/184 khung). Thứ tự trip trong `gold_dataset_frames.sql` là `md5(trip_id || LAB_STUDENT_ID)`, nên MSSV đổi thì trip nào vào train/val/test đổi, kéo theo version đổi. Tôi thử chạy bước 4 với MSSV `2024000`: version thành **`8fc131884584`** và split thành 703/182/134; đặt lại `2A202602862` thì quay về `59cc937c0594`. Cùng dữ liệu nhưng khác MSSV → khác split → khác version.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

`lake/gold/datasets/59cc937c0594/manifest.json`, phần `lineage`:

```
bronze.raw_telemetry_a: 0, bronze.raw_telemetry_b: 0, bronze.trips_raw: 0, bronze.video_files: 1,
silver.frames: 27, silver.telemetry: 0, silver.frames_flagged: 0, silver.frame_telemetry: 0,
input_video_sha256: drive_a 636ba1a98dfc5ba6..., drive_b bfa62703f645ff77...
```

Tức là dataset `59cc937c0594` được tạo từ hai video gốc có sha256 trên, telemetry bronze A/B ở version 0, và các bảng silver ở đúng các version Delta kể trên (`silver.frames` version 27). Manifest còn ghi `rules` (bỏ `is_black, is_blurry, is_near_dup, not sync_ok`; split "per trip, stratified by drive, md5 order"), `manifest_sha256` `b5aa18d0...` và số khung theo split/drive.

3 tháng sau, để tái tạo: dùng **time travel** của Delta đọc các bảng ở đúng version trong `lineage`, kiểm tra video đầu vào bằng `input_video_sha256`, dùng cùng mã (commit git) và tham số dbt (`dup_hamming=2`, `sync_tol_ms=150`, `split_train=0.70`...), cùng `LAB_STUDENT_ID` (salt của split); kết quả đúng nếu băm ra lại `59cc937c0594`. Lưu ý: cần giữ lịch sử Delta (không `VACUUM` các version cũ) thì time travel mới làm được.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Ước lượng: 500 xe × vài giờ video/ngày → hàng nghìn giờ video, hàng chục triệu khung hình mỗi ngày, so với 1246 khung của lab.
1. **Giải mã video bằng FFmpeg trong một container Airflow** (slide 24: decode là nút cổ chai). Ở lab bước 3 đã mất nhiều phút cho 10 phút video trên máy 4 GB RAM. Thay bằng xử lý phân tán: worker GPU (NVDEC) chạy trên Kubernetes/Ray/Spark, mỗi trip/clip là một job, đọc thẳng từ object storage.
2. **DuckDB một tiến trình + một file `lakehouse.duckdb`** cho mọi phép biến đổi dbt (slide 22, ELT). Dữ liệu và join ASOF sẽ vượt RAM một máy và không chạy song song được. Thay bằng engine phân tán (Spark/Databricks, Trino) đọc Delta trên S3, giữ dbt làm lớp mô hình hoá; bảng partition theo ngày/xe.
3. **Airflow với LocalExecutor, mỗi trip một mapped task chạy tuần tự** (`max_active_tis_per_dagrun=1`). 26 task thì ổn, hàng chục nghìn trip/ngày sẽ làm nghẽn scheduler và metadata DB. Thay bằng CeleryExecutor/KubernetesExecutor, DAG theo partition ngày (backfill theo ngày), gom trip theo lô, hoặc ingest theo sự kiện (slide 12/23: streaming, Kafka) khi xe tải dữ liệu lên.
Ngoài ra MinIO một node → S3/cluster MinIO nhiều node, và Metabase gallery đọc ảnh trực tiếp sẽ cần CDN/thumbnail.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
