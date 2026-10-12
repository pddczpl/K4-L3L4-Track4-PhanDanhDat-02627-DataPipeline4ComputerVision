# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Phan Danh Đạt |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 02627 |
| GitHub | pddczpl |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-00722 |

## Kết quả cuối

- `dataset_version` của bạn: `b7d9cec7d2a7`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `717 / 155 / 147` (tổng 1019 khung; số trip tương ứng: 18 / 4 / 4)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (`airflow_crash_task.png` log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Bốn khác biệt chính giữa telemetry nguồn A và nguồn B ở tầng Bronze:
1. Tên cột: Nguồn A sử dụng các cột chuẩn tiếng Anh (`ts_utc`, `speed_mps`, `heading_deg`, `acc_x`, `acc_y`, `steer`), trong khi nguồn B dùng tên viết tắt và khác biệt (`timestamp_ms`, `spd_kmh`, `hdg`, `ax`, `ay`, `steering`).
2. Đơn vị đo lường: Vận tốc ở nguồn A được đo bằng `m/s`, nguồn B đo bằng `km/h`. Ngoài ra nguồn B còn có thêm dữ liệu bàn đạp ga/phanh theo tỷ lệ phần trăm (%).
3. Định dạng thời gian: Nguồn A dùng chuỗi chuẩn ISO 8601 UTC dạng chuỗi (ví dụ `2025-10-08T14:00:00Z`), còn nguồn B dùng số nguyên Unix Epoch tính theo mili-giây (milliseconds).
4. Độ lệch đồng hồ thiết bị (Clock drift): Nguồn A đồng bộ chuẩn (offset 0 ms), trong khi đồng hồ cảm biến nguồn B bị chạy nhanh hơn thực tế 1.5 giây (+1500 ms) so với video camera.

Pipeline giữ mỗi nguồn một bảng Bronze riêng và lưu toàn bộ cột ở dạng chữ (text) vì tầng Bronze tuân thủ nguyên tắc "Ghi nguyên bản, chỉ thêm" (Raw Ingestion / Append-only). Thiết bị ngoại vi thường xuyên bị biến động cấu trúc (schema drift) hoặc sinh dữ liệu bất thường; nếu gộp và ép kiểu ngay tại Ingestion, pipeline sẽ lập tức bị sập khi gặp lỗi parse kiểu dữ liệu, làm mất mát dữ liệu thô. Lưu dạng chuỗi nguyên bản đảm bảo pipeline luôn nuốt trọn dữ liệu an toàn, cho phép audit và tái xử lý khi luật biến đổi thay đổi.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Pipeline đưa hai nguồn về một schema chung thông qua các dbt staging models (`stg_telemetry_a.sql` và `stg_telemetry_b.sql`):
- Đơn vị vận tốc được quy đổi đồng nhất về `m/s` (nguồn B lấy `spd_kmh / 3.6`).
- Thời gian được chuẩn hóa về kiểu TIMESTAMP WITH TIME ZONE (UTC), đồng thời nguồn B được trừ đi đúng 1500 ms độ lệch đồng hồ (`timestamp_ms - 1500`).
- Bằng chứng đã trừ độ lệch đồng hồ: Timestamp của bản ghi hợp lệ đầu tiên ở drive B là `2025-10-08 14:30:00.009000+00:00`, khớp chính xác với mốc video bắt đầu (`2025-10-08 14:30:00`), kiểm tra `verify --module 2` đạt PASS với độ chênh lệch `< 0.5 s`.

Bảng cách ly (Quarantine) từ `run_summary.json`:
| drive_id | duplicate | speed_missing | speed_out_of_range | Tổng rejects | Tổng nhận về (clean + rejects) |
|---|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 53 | 2968 + 53 = 3021 dòng |
| drive_b | 20 | 30 | 3 | 53 | 3181 + 53 = 3234 dòng |

Các dòng lỗi được đưa vào bảng `silver.silver_telemetry_rejects` kèm theo lý do cụ thể (`reject_reason`) thay vì bị xóa im lặng (silent drop) nhằm bảo vệ tính minh bạch dữ liệu. Việc lưu lại cho phép đội ngũ kỹ sư giám sát tỷ lệ suy hao cảm biến (Sensor degradation / Error budget), phục vụ điều tra nguyên nhân gốc rễ (RCA) và cho phép khôi phục khi luật kiểm tra được điều chỉnh mà không làm ô nhiễm tập dữ liệu sạch chính.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

Test `max_gap_ms` là `warn` vì hiện tượng mất kết nối tạm thời giữa cảm biến với bộ ghi (ví dụ khi xe đi qua hầm hoặc cầu vượt) là hiện tượng vật lý bình thường ngoài đời thực. Dữ liệu giữa các khoảng trống vẫn có giá trị sử dụng, cảnh báo giúp kỹ sư lưu ý về độ trễ đo đạc mà không làm gián đoạn toàn bộ pipeline. Ngược lại, test `in_range` (tốc độ từ 0 đến 70 m/s) là `error` vì ô tô dân dụng không thể có vận tốc âm hoặc vượt quá 250 km/h; dữ liệu sai lệch vô lý này nếu đưa vào huấn luyện mô hình xe tự lái sẽ gây ra các hành vi điều khiển thảm họa, do đó bắt buộc phải chặn đứng pipeline ngay lập tức.

Kết quả thí nghiệm `break-gate` (`report/evidence/break_gate.json`):
- Khi nới luật làm sạch từ 70 m/s lên 1000 m/s: Cổng chất lượng dbt phát hiện ra đúng **6 dòng** vi phạm (mỗi drive có 3 dòng speed_out_of_range lọt qua).
- Test bắt được lỗi: `in_range_silver_telemetry_speed_mps__70__0`.
- Tóm tắt kết quả: Khi nới luật -> `PASS=5 WARN=1 ERROR=1 SKIP=0` (pipeline bị chặn đứng); sau khi khôi phục luật gốc -> `PASS=6 WARN=1 ERROR=0 SKIP=0` (toàn bộ cổng chất lượng xanh trở lại).
Điều này khẳng định vai trò cốt lõi của "cổng chất lượng" (quality gate): đóng vai trò như một chốt chặn bảo vệ chiều sâu (defense-in-depth), đảm bảo ngay cả khi tầng upstream có sai sót hoặc nới luật bất cẩn, dữ liệu bẩn và dị thường cũng không bao giờ lọt vào kho dữ liệu hạ nguồn và mô hình ML.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Dựa vào kết quả thí nghiệm `idempotency.json`:
- Khi chạy lại phân vùng `trip-00003` theo chuẩn pipeline:
  + Số dòng trước khi chạy: 1246 dòng, mã vân tay nội dung: `6c7480dfde7e`.
  + Số dòng sau 2 lần chạy lại: vẫn chính xác **1246 dòng**, mã vân tay: `6c7480dfde7e` (hoàn toàn không đổi).
- Khi ghi sai theo kiểu `append`: Đối với trip có 60 khung hình, sau 2 lần chạy số lượng đã tăng vọt lên **120 dòng** (dữ liệu bị nhân đôi 2x).

Delta `overwrite` theo partition (`replaceWhere: trip_id = '...'`) an toàn vượt trội so với "xoá rồi ghi lại" nhờ cơ chế giao dịch ACID nguyên tử (atomic commit). Delta ghi các file Parquet dữ liệu mới vào lưu trữ trước, chỉ khi ghi thành công toàn bộ mới thực hiện một commit duy nhất vào `_delta_log`. Nếu giữa chừng hệ thống bị mất điện, hết bộ nhớ hoặc crash: commit chưa diễn ra, trạng thái bảng vẫn giữ nguyên vẹn dữ liệu cũ. Ngược lại, nếu chọn cách "xoá rồi ghi lại", nếu gặp lỗi sau lệnh xoá hoặc đang ghi dở, toàn bộ partition đó sẽ biến mất hoặc bị lỗi phân nửa, làm gián đoạn toàn bộ hệ thống đọc đồng thời.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung hình **đen** (`is_black`, độ sáng trung bình luma < 12) là lỗi phần cứng/sensor (nắp camera bị che, camera mất nguồn, lỗi đường truyền), hoàn toàn không chứa thông tin thị giác nên pipeline **loại bỏ** khỏi Gold. Khung hình **tối** (`is_dark`, `dark_kept`: drive A có 4 khung, drive B có tới 61 khung) là môi trường ánh sáng yếu tự nhiên (xe đi trong đường hầm, bóng râm cây cối, chập tối); khung này vẫn chứa các đặc trưng quan trọng về lòng đường và xe cộ. Pipeline **gắn cờ cảnh báo nhưng giữ lại** trong Gold. Tôi hoàn toàn đồng ý vì xe tự lái thực tế bắt buộc phải có dữ liệu huấn luyện trong điều kiện thiếu sáng để phản xạ an toàn.

Tôi chọn **ngưỡng 2 bit** (ngưỡng pipeline đang dùng):
- Ngưỡng 0 bit quá khắt khe: chỉ bắt được 61 khung ở drive A và 8 khung ở drive B, bỏ lọt các khung đứng hình 6 giây nhưng có nhiễu cảm biến nhẹ (sensor noise).
- Ngưỡng 6 bit quá rộng: loại bỏ tới 363 khung ở A (hơn 60%) và 244 khung ở B, dẫn đến xóa nhầm cả các khung hình có chuyển động cảnh quan thực sự.
Ngưỡng này phụ thuộc chặt chẽ vào tốc độ xe: Khi xe dừng đỗ hoặc chạy rất chậm (ví dụ `trip-08888` chỉ 6.5 km/h), các khung cách nhau 0.5s gần như không đổi cảnh nên dHash chênh lệch rất ít bit; ngược lại khi xe chạy nhanh trên cao tốc (70-90 km/h), cảnh quan biến đổi liên tục tạo ra khoảng cách Hamming lớn hơn rất nhiều giữa các khung liên tiếp.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Dựa vào kết quả thí nghiệm `report/evidence/leakage.json`:
- Khi chia ngẫu nhiên THEO KHUNG HÌNH (tỷ lệ 70/15/15): Toàn bộ **26/26 trip** bị chia cắt trên nhiều split, và cả **26/26 trip đều có khung hình xuất hiện đồng thời ở cả tập train lẫn test**.
- Khi chia THEO TRIP (chuẩn Gold của pipeline): **0 trip** bị chia cắt (mỗi trip nằm trọn vẹn trong duy nhất một split: train, val hoặc test).

Điều này làm điểm kiểm thử (test accuracy/loss) đẹp một cách giả tạo (Data Leakage) vì video được lấy mẫu ở tần số cao (2 khung/giây, cách nhau chỉ 500 ms). Hai khung hình liên tiếp trong cùng một chuyến đi có bối cảnh không gian, góc nhìn camera, hướng ánh sáng và phương tiện xung quanh gần như giống hệt nhau. Nếu chia theo khung hình, mô hình đã "học thuộc lòng" khung hình ở thời điểm $t$ trong tập train, nên khi gặp khung hình $t + 0.5s$ ở tập test, mô hình dự đoán chính xác dễ dàng như nhìn thấy trước đề thi. Tuy nhiên khi triển khai ngoài thực tế trên một cung đường hoàn toàn mới (unseen scene), mô hình sẽ sụp đổ hiệu năng vì không học được khả năng tổng quát hóa thực sự.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` được thiết kế là mã băm nội dung (content hashing) thay vì ngày giờ hay số đếm tự tăng nhằm đảm bảo tính xác định và khả năng tái lập (reproducibility):
- Nếu sử dụng ngày giờ hoặc số đếm, mỗi lần chạy pipeline trên cùng một tập dữ liệu sẽ sinh ra một phiên bản mới dù nội dung không đổi một byte nào, gây lãng phí dung lượng lưu trữ và mất tính liên kết mô hình.
- Sử dụng mã băm nội dung (hash của danh sách frame_id, split gán, metadata) đảm bảo: cùng nội dung thì luôn ra cùng một mã hash duy nhất. Hai lần chạy liên tiếp của tôi đều cho ra chính xác cùng một phiên bản: `b7d9cec7d2a7`.
- `dataset_version` của tôi **khác bạn bè** dù cùng nguồn dữ liệu vì pipeline sử dụng MSSV (`LAB_STUDENT_ID=02627`) làm giá trị seed (muối - salt) để chia các trip vào train/val/test. Do MSSV mỗi người là duy nhất, tập các trip và khung hình phân bổ vào các split của tôi là riêng biệt (train 717, val 155, test 147), dẫn đến mã hash nội dung `dataset_version` của mỗi người là duy nhất.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Trích xuất trực tiếp từ trường `lineage` trong `gold/datasets/b7d9cec7d2a7/manifest.json`:
- Nguồn video thô gốc: Hai file video có mã băm SHA-256 xác thực:
  + `drive_a`: `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`
  + `drive_b`: `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`
- Phiên bản bảng thượng nguồn (Delta table commit version):
  + `bronze.raw_telemetry_a`: version 0
  + `bronze.raw_telemetry_b`: version 0
  + `bronze.trips_raw`: version 0
  + `bronze.video_files`: version 1
  + `silver.frames`: version 46 (sau các lần extract trip và rerun)
  + `silver.telemetry`: version 0, `silver.frames_flagged`: version 0, `silver.frame_telemetry`: version 0

Nếu 3 tháng sau có người hỏi, các thông tin then chốt giúp tái tạo lại chính xác 100% dataset này bao gồm:
1. Hai file video gốc có đúng mã SHA-256 nêu trên.
2. Tính năng Time Travel của Delta Lake để truy vấn các bảng thượng nguồn chính xác tại số commit version tương ứng ghi trong lineage (ví dụ `VERSION AS OF 46` cho `silver.frames`).
3. Cùng bộ quy tắc lọc (luma < 12, blur < 40, dhash <= 2, sync_tol_ms <= 150) và cùng giá trị MSSV `02627` để chia split.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Mô tả chính xác quá trình sập và khôi phục của tôi:
- Khi đưa cấu hình mô phỏng crash `LAB_FAIL_ONCE=trip-00722` (trip tương ứng với MSSV 02627) và chạy Airflow: đúng **1 task** chuyển đỏ (`FAILED`), đó là task `silver_frames` với `map_index = 1`. Log hiển thị rõ ràng: `RuntimeError: simulated crash while extracting trip-00722; run it again`.
- Đúng **7 task** hạ nguồn chuyển sang trạng thái `upstream_failed`: `dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse`.
- Thao tác khôi phục: Tôi thực hiện **Clear** đúng task `silver_frames` (`map_index = 1`) trên giao diện Airflow.
- Kết quả chạy lại: Task `silver_frames` chạy lại thành công, kéo theo toàn bộ 7 task downstream thực thi tiếp và cả DAG chuyển xanh hoàn toàn. Dữ liệu **hoàn toàn không bị nhân đôi** nhờ cơ chế `overwrite` partition của Delta Lake (tổng vẫn là 1246 frames ở Silver). `dataset_version` **hoàn toàn không đổi**, vẫn giữ nguyên `b7d9cec7d2a7`.
- Bằng chứng:
  + Ảnh log task đỏ ghi rõ lỗi của `trip-00722`: `report/screenshots/airflow_crash_task.png`.
  + Ảnh DAG xanh toàn bộ sau khi clear: `report/screenshots/airflow_graph.png`.
  + File kiểm tra `report/verify.txt` đạt tuyệt đối `27/27 checks passed`.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Khi mở rộng quy mô lên 500 xe/ngày (tương đương ~20-50 TB video và hàng trăm triệu dòng telemetry mỗi ngày), ba thành phần sẽ vỡ đầu tiên:
1. **Giải mã video bằng FFmpeg cục bộ (Video decoding & feature extraction - slide 24)**:
   - Hiện tại chạy FFmpeg tuần tự/bán tuần tự trên một container duy nhất. Với 500 xe, CPU và I/O ổ đĩa của 1 máy sẽ nghẽn nghiêm trọng (decode video luôn là nút thắt cổ chai lớn nhất).
   - Thay thế: Chuyển sang kiến trúc xử lý phân tán quy mô lớn sử dụng **Apache Spark trên Kubernetes** hoặc cụm **Ray Core/Ray Data** kết hợp GPU decoding (NVDEC hardware acceleration) để decode song song hàng nghìn video chunk độc lập.
2. **DuckDB chạy trên một node cục bộ (Single-node ELT engine - slide 22, 23)**:
   - Hiện tại DuckDB thực hiện toàn bộ phép join, deduplicate và dbt transformation trên một file `lakehouse.duckdb`. Với hàng tỷ điểm đo telemetry, DuckDB sẽ cạn kiệt RAM (OOM) và không thể scale-out ra nhiều máy.
   - Thay thế: Chuyển sang Lakehouse compute engine phân tán như **Apache Spark (Dataproc/Databricks)** hoặc **Google Cloud BigQuery (BigLake)** có khả năng co giãn linh hoạt và truy vấn trực tiếp trên Delta Lake/Iceberg quy mô Petabyte.
3. **Điều phối Airflow LocalExecutor với Dynamic Task Mapping trên một máy (Slide 26, 27)**:
   - Hiện tại Airflow sinh 26 task dynamic mapping trên cùng một container. Với 500 xe, việc chia theo trip sẽ tạo ra hàng vạn task instance mỗi ngày, gây nổ hàng đợi metadata database (task queue explosion) và làm treo scheduler.
   - Thay thế: Chuyển sang **Airflow KubernetesExecutor / CeleryExecutor** để phân tán worker; đồng thời gom nhóm trip thành các batch công việc lớn (micro-batching) và ủy quyền tính toán cho Spark Operator thay vì để Airflow trực tiếp điều phối từng trip nhỏ lẻ.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

```sql
select 
    trip_id,
    drive_id,
    lighting,
    conditions,
    split,
    frames as total_frames,
    frames_kept,
    (frames - frames_kept) as frames_dropped,
    round(100.0 * (frames - frames_kept) / nullif(frames, 0), 2) as drop_rate_pct,
    avg_speed_kmh
from marts.mart_trip_summary
order by drop_rate_pct desc
limit 10;
```

**Điều câu truy vấn cho biết**:
Câu truy vấn này phân tích chất lượng của từng chuyến đi (trip) thông qua tỷ lệ phần trăm khung hình bị loại bỏ (`drop_rate_pct`) trên bảng `marts.mart_trip_summary`, kết hợp với điều kiện ánh sáng (`lighting`), thời tiết (`conditions`), tập `split` và tốc độ trung bình `avg_speed_kmh`. Kết quả trên dữ liệu của tôi cho thấy `trip-04622` (drive_a) có tỷ lệ loại bỏ cao nhất lên tới **56.67%** (chỉ giữ lại 26/60 khung hình), tiếp theo là `trip-03904` (50.0%) và `trip-02086` (46.67%). Khi đối chiếu vận tốc, các trip có tỷ lệ loại bỏ cao thường rơi vào các đoạn xe di chuyển cực kỳ chậm (như `trip-08888` chỉ 6.5 km/h bị loại 40% do camera đứng yên tạo ra nhiều khung trùng lặp) hoặc các đoạn camera bị lỗi đứng hình 6 giây. Đây là chỉ số vận hành quan trọng giúp kỹ sư phát hiện sớm các chuyến đi có vấn đề về phần cứng camera hoặc điều kiện lái xe bất thường trước khi đưa vào tập huấn luyện.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Điều làm tôi bất ngờ nhất là tính ưu việt của kiến trúc Lakehouse với Delta Lake: việc kết hợp cơ chế lưu trữ đối tượng S3/MinIO với giao dịch ACID `replaceWhere` cho phép khôi phục task bị sập một cách mượt mà, bảo đảm tính idempotent tuyệt đối mà không cần can thiệp dọn dẹp thủ công. Điều tôi muốn tìm hiểu sâu hơn là cách tối ưu hóa chi phí và băng thông mạng khi shuffle dữ liệu video ở quy mô hàng nghìn camera phân tán. Một lỗi dữ liệu thực tế mà tôi nhận thấy pipeline hiện tại **chưa bắt được** là trường hợp **camera bị bẩn hoặc bị bùn đất/nước mưa che khuất cục bộ một góc ống kính** (partial lens occlusion): do độ sáng trung bình và phương sai độ nét toàn khung hình vẫn nằm trong ngưỡng chấp nhận, các khung hình này vẫn lọt qua tầng Silver vào Gold. Trong tương lai, cần bổ sung mô hình deep learning nhẹ kiểm tra dị vật trên ống kính tại cổng chất lượng.
