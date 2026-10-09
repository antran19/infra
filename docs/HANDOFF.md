# Project Nexus — Handoff / Trạng thái hiện tại

**Cập nhật lần cuối:** 2026-10-09. File này tồn tại để 1 session Claude Code mới đọc xong là
nắm được toàn bộ trạng thái dự án **mà không cần đọc lại từng file code** — tốn ít token hơn
nhiều so với tự khám phá lại từ đầu. Khi có thay đổi lớn (thêm service, đổi kiến trúc, fix bug
quan trọng), **cập nhật file này** thay vì để nó lỗi thời như lần trước (bản cũ dừng ở lúc
auction-service còn chưa viết dòng code nào, trong khi thực tế đã xong rất nhiều).

## 1. Bối cảnh

Capstone/đồ án FPT, domain **e-commerce có chức năng đấu giá**. SRS gốc:
`C:\FPT\srs-nexus-ecommerce-auction-v1.docx` (đã trích text ra, xem cách làm ở mục 7 nếu cần
đọc lại — máy này không có `pandoc`/`soffice`, phải dùng cách unzip + strip XML thủ công).

Nhóm 4 người, leader là user của conversation này (Trần Nguyễn Minh An).

**2 nơi lưu code song song, phải đồng bộ thủ công:**
- **GitHub polyrepo** (`antran19/<service>`) — nơi code thật sự được viết trong mọi session.
- **GitLab monorepo** (`gitlab-group2-nexus`, remote
  `git.fsoft-academy.edu.vn/hcm26_cpl_java_11/hcm26_cpl_java_11_group_2`) — nơi nộp bài, mỗi
  service nằm trong 1 thư mục con. **Không tự động sync** — sau mỗi đợt thay đổi đáng kể,
  phải tự kéo code từ GitHub vào đây bằng `git subtree pull --prefix=<service> <github-url>
  main --squash` (dùng `subtree add` nếu service đó chưa từng có trong monorepo). Lưu ý
  GitLab qua Cloudflare hay lỗi `HTTP/2 PROTOCOL_ERROR` khi push file lớn — đã fix cố định
  bằng `git config http.version HTTP/1.1` + `http.postBuffer 524288000` (repo-local config,
  đã set sẵn trong `gitlab-group2-nexus`, không cần set lại).

## 2. Các repo (clone làm sibling trong `C:\FPT`)

```
C:\FPT\
  common-libs/        # 4 module dùng chung, version hiện tại 1.4.0
  discovery-server/    # Eureka, port 8761
  api-gateway/          # Spring Cloud Gateway (WebFlux), port 8080
  user-service/         # port 8081, DB user_db (postgres port 5432)
  catalog-service/      # port 8082, DB catalog_db (5433)
  auction-service/      # port 8083, DB auction_db (5434)
  notification-service/ # port 8084, DB notification_db (5435)
  commerce-service/     # port 8085, DB commerce_db (5436)
  infra/                 # docker-compose cho cả cụm + file này
  nexus-frontend/        # React+Vite+Tailwind, FE của leader. API.md ở root = tài liệu API
                          # đầy đủ cho FE, LUÔN cập nhật file đó song song khi đổi API.
  gitlab-group2-nexus/   # bản sync sang GitLab, xem mục 1
```

Có 1 repo **solo** không liên quan (`antran19/project-nexus`) — bỏ qua, không phải hướng đang
làm.

## 3. Kiến trúc & convention (áp dụng cho MỌI service, đọc kỹ trước khi thêm code mới)

- **Hexagonal**: `api/` (controller, DTO, MapStruct mapper) → `application/usecase/` (class
  thường, không Spring annotation, constructor injection) + `application/port/out/` (interface)
  → `domain/model/` (immutable, private constructor + factory `create`/`reconstitute`, method
  `withX()` trả instance mới) + `domain/service/` (business rule thuần, không framework) →
  `infrastructure/persistence|messaging|config|payment` (implement port).
- **Use case wiring**: tất cả qua `infrastructure/config/UseCaseConfig.java`, `@Bean` method
  thủ công, không `@Service`/`@Component` trên use case.
- **`common-libs`** (4 module, publish qua GitHub Packages khi push lên `main`):
  `common-core` (`ApiResponse<T>`, exception hierarchy: `NotFoundException`/
  `ConflictException`/`ForbiddenException`/`ValidationException`/`UnauthorizedException`,
  mỗi cái constructor `(errorCode, message)`), `common-web` (`GlobalExceptionHandler`),
  `common-security` (`@RequiresPrivilege`, `JwtTokenProvider`, `JwtAuthenticationFilter`,
  `TokenDetails` — xem mục 5), `common-events` (`DomainEvent` base + mọi event class). **Sửa
  gì trong common-libs phải bump version + `mvn clean install -DskipTests` để các service
  khác build local được, rồi push để CI publish lên GitHub Packages.**
- **Outbox pattern**: mọi service publish event qua bảng `outbox` (ghi cùng transaction với
  nghiệp vụ) + `OutboxRelayJob` (`@Scheduled(fixedDelay=5000)`) đẩy lên Kafka topic
  `<service>-events`. Service nào cần nghe thì thêm `@KafkaListener(topics="...")`, parse
  `eventType` bằng Jackson, bỏ qua event không quan tâm.
- **Response envelope**: `{success, data, error}`, `error.fieldErrors` cho lỗi validate field.
- **Privilege**: `@RequiresPrivilege("X.Y")` ở method controller, check qua JWT claim
  `privileges`. Mọi endpoint ghi dữ liệu cần JWT hợp lệ (`anyRequest().authenticated()`); GET
  public trừ khi đụng dữ liệu riêng tư (cart/order/reputation chi tiết) thì vẫn cần
  `@RequiresPrivilege`.
- **Cross-service ID**: luôn là UUID string "mù" (opaque) — **không có service nào gọi đồng bộ
  sang service khác qua REST**. Mọi phối hợp giữa service đều qua Kafka event.
- **TDD bắt buộc**: viết test trước, chạy thấy RED, code tới khi GREEN, rồi mới sang việc tiếp.
  Unit test cho domain/use case dùng Mockito, không cần Spring. Integration test cho
  repository adapter/outbox dùng **Testcontainers thật** (Postgres/Kafka), không mock DB.
- **⚠️ Bug môi trường đã gặp**: `mvn clean` trên máy Windows này **đôi khi không xoá sạch
  `target/`** (nghi file bị khoá bởi tiến trình nền/IDE), để lại `.class` cũ không khớp source
  mới (ví dụ MapStruct impl thiếu `implements`) → lỗi khó hiểu kiểu "No qualifying bean". Nếu
  gặp lỗi Spring context load thất bại mà code nhìn đúng, **xoá tay `rm -rf target` rồi build
  lại** trước khi nghi ngờ gì khác.

## 4. Trạng thái từng service (audit thật từ code ngày 2026-10-09, không suy đoán)

### user-service (port 8081)
✅ Đăng ký, Login (JWT), đổi mật khẩu tự thân, luồng nâng cấp BUYER→SELLER (request + admin
duyệt/từ chối), **Reputation module đầy đủ**: rating sau giao dịch, điểm uy tín + trust level
(LOW<40/NORMAL 40-49/TRUSTED≥50), tự động trừ điểm khi bùng kèo đấu giá (nghe
`AuctionPaymentTimeout` qua Kafka), `trustLevel` nhúng vào JWT lúc login.

❌ **Thiếu hoàn toàn** dù SRS yêu cầu và privilege đã seed sẵn: Admin CRUD user (create/update
/delete/list/view người dùng khác), Role management (tạo/sửa/xoá/liệt kê role — chỉ seed được
qua Flyway, không có API), Logout, Forget password. Admin điều chỉnh thủ công điểm uy tín
(`USER.REPUTATION.ADJUST`) cũng chưa có — hiện chỉ tự động trừ điểm, không ai chỉnh tay được.
Dispute handling (SRS có nhắc) — chưa có gì.

### catalog-service (port 8082)
✅ Product CRUD + đổi trạng thái (DRAFT/ACTIVE/INACTIVE) + search (Postgres full-text search,
filter q/categoryId/status/sellerId) + discover (trang chủ). Category CRUD đầy đủ.

⚠️ `GET /discover` chỉ lọc `status=ACTIVE`, chưa có logic "phổ biến"/"sắp hết giờ đấu giá" như
SRS mô tả.

### auction-service (port 8083)
✅ Đầy đủ nhất trong toàn hệ thống: tạo/sửa/huỷ đấu giá, lifecycle tự động
(`AuctionLifecycleJob`, PENDING→ACTIVE→ENDED), đặt giá với lock chống race condition
(`SELECT...FOR UPDATE`), anti-sniping (tự gia hạn), xác định người thắng, payment deadline
(`PaymentDeadlineJob`, 24h, tự phát `AuctionPaymentTimeout` nếu quá hạn — **đã fix bug: không
còn phạt nhầm auction đã thanh toán**), **enforce `trustLevel` thật** (LOW không đặt giá được,
dưới TRUSTED không tạo đấu giá được được — đọc trực tiếp từ JWT, không gọi user-service).
Payment đã chuyển hẳn sang commerce-service xử lý (auction-service chỉ phát event, không tự
tạo order/gọi Stripe nữa).

❌ Chưa có: auction riêng tư (visibility public/restricted), check tường minh
`MAX_ACTIVE_AUCTIONS_PER_SELLER` trong use case (có field nhưng chưa thấy enforce — **cần xác
minh lại**, audit trước có thể sai chỗ này).

### commerce-service (port 8085)
✅ Giỏ hàng CRUD, checkout (giỏ→order), tạo order tự động từ `AuctionWonEvent` (Kafka), thanh
toán Stripe thật (checkout session + confirm, idempotent), huỷ order (chỉ khi chưa thanh toán).

⚠️/❌ **Không có refund thật** (huỷ order đã thanh toán chỉ là out-of-scope có chủ đích, chưa
code), **không có invoice/receipt**, **không có Admin xem tất cả order** (chỉ xem order của
chính mình), cart không re-validate giá/tồn kho lúc checkout, chưa có cart-expiration job.
**🔴 BUG: `api-gateway` chưa có route cho commerce-service** — gọi qua gateway (`:8080`) sẽ
không tới được, phải gọi thẳng `:8085`. Cần sửa `api-gateway/src/main/resources/application.yml`.

### notification-service (port 8084)
✅ Tự động ghi log mọi event nghe được từ Kafka (`auction-events`/`catalog-events`/
`user-events`... — **cần thêm nghe `commerce-events` nếu chưa có, chưa verify**), xem lịch sử
thông báo của mình, Admin xem toàn bộ (audit).

❌ **Không gửi email/push thật** — chỉ lưu DB, không có channel gửi đi nào. Không có preference
người dùng. Không có retry/delivery-status. Không có health-check cho message broker (SRS yêu
cầu). Không có springdoc/Swagger (các service khác đều có).

### Fulfillment Service
❌ **0% — không có 1 dòng code, không có repo.** SRS mục 3.6 (Inventory/Warehouse + Shipping)
hoàn toàn chưa động tới.

### Hạ tầng
`discovery-server`/`api-gateway`/`common-libs`/`infra` (docker-compose) hoạt động tốt, đã
verify end-to-end nhiều lần qua Docker thật (kể cả thanh toán Stripe thật qua trình duyệt).
API versioning `/api/v1` nhất quán. Swagger có ở user/catalog/auction/commerce-service, thiếu
ở notification-service.

## 5. Cơ chế JWT (quan trọng nếu đụng tới auth)

JWT claim gồm: `sub` (userId), `role`, `privileges` (list), `trustLevel` (LOW/NORMAL/TRUSTED,
tính từ điểm uy tín **tại thời điểm login** — đổi điểm sau đó không có hiệu lực tới khi login
lại/token hết hạn, 60 phút). `JwtAuthenticationFilter` (common-security) set
`Authentication.getPrincipal()` = userId, authorities = privileges, và
`Authentication.getDetails()` = `TokenDetails(role, trustLevel)` — service nào cần đọc
`trustLevel` (hiện chỉ auction-service) thì cast `getDetails()` sang `TokenDetails`. Thiếu
claim (token cũ) → fail-open (không chặn), xem comment trong `PlaceBidUseCase`/
`CreateAuctionUseCase`.

## 6. Việc đang làm / tiếp theo (thứ tự ưu tiên đã thống nhất với user, 2026-10-09)

1. ⬜ Sửa route `api-gateway` cho commerce-service (bug, không phải thiếu tính năng).
2. ⬜ Admin User CRUD + Role management ở user-service.
3. ⬜ Logout + Forget Password.
4. ⬜ Phần lớn hơn, chưa chốt phạm vi chi tiết: refund thật, invoice, notification gửi email/
   push thật, Fulfillment — làm tới đâu tính tới đó, KHÔNG tự ý làm hết 1 lượt vì quy mô lớn.

*(Đánh dấu ✅ khi xong, cập nhật ngày + tóm tắt ngắn ở đây thay vì để trạng thái cũ.)*

## 7. Đọc lại SRS gốc nếu cần

File `.docx` không đọc trực tiếp được (máy này thiếu `pandoc`/`soffice`). Cách đã dùng:
`unzip -oq "C:\FPT\srs-nexus-ecommerce-auction-v1.docx" -d <scratchpad>/srs_unpacked/`, rồi
chạy script Python strip tag XML trên `word/document.xml` → text thuần (~59000 ký tự). Toàn bộ
nội dung SRS (mục 2.7 bảng privilege, mục 3.1-3.6 requirement chi tiết, mục 5 NFR) đã được đọc
và đối chiếu với code thật ít nhất 1 lần (2026-10-09) — xem mục 4 ở trên là kết quả, không cần
đọc lại SRS từ đầu trừ khi cần tra câu chữ chính xác.
