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
  fulfillment-service/  # port 8086, DB fulfillment_db (5437) -- Inventory core, standalone
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

✅ (2026-10-09) **Admin User CRUD + Role management** — `create/update/delete(soft)/list/view`
user (`USER.CREATE/UPDATE/DELETE/LIST/VIEW`), admin đổi mật khẩu người khác không cần mật khẩu
cũ (`USER.CHANGE_PASSWORD`), Role `create/update/delete/list/view` (`ROLE.CREATE/UPDATE/DELETE
/LIST/VIEW`) kèm gán privilege, xoá role bị chặn nếu còn user tham chiếu (409
`ROLE_IN_USE`). Soft-delete qua cột `deleted_at` (giữ FK cho rating/order, loại khỏi
login/view/list). Migration `V11`. 98 test, verify sống đầy đủ qua Docker (xem commit
`94a6cc9`).

✅ (2026-10-09) **Logout + Forget/Reset Password** — Logout blacklist `jti` của token hiện
tại (bảng `blacklisted_tokens`), enforce qua `TokenBlacklistPort` mới trong common-libs
1.5.0 (`JwtAuthenticationFilter` check optional bean này; service nào không wire bean thì
fail-open — **chỉ user-service enforce được, token logout vẫn còn hiệu lực ở
catalog/auction/commerce-service tới khi tự hết hạn (60 phút)** — hạn chế đã biết, không
làm blacklist phân tán qua Kafka vì quá tốn cho scope hiện tại). Forget password: token
one-time 30 phút, hash SHA-256 (không bcrypt vì token đã đủ entropy, cần lookup theo hash
trực tiếp), trả `rawToken` thẳng trong response tạm thời (**chưa có email thật** — xem mục
4). Reset password validate token chưa dùng/chưa hết hạn trước khi đổi mật khẩu. Migration
`V12`. Verify sống đầy đủ qua Docker (login→gọi API→logout→gọi lại bị 401; forgot→reset→
login mật khẩu mới OK/mật khẩu cũ fail/token dùng lại bị 401). Commit `f9a8fc3`
(user-service) + `efbc581` (common-libs).

❌ **Thiếu**: Admin điều chỉnh thủ công điểm uy tín (`USER.REPUTATION.ADJUST`) — hiện chỉ tự
động trừ điểm, không ai chỉnh tay được. Dispute handling (SRS có nhắc) — chưa có gì.

### catalog-service (port 8082)
✅ Product CRUD + đổi trạng thái (DRAFT/ACTIVE/INACTIVE/**SOLD** mới thêm) + search (Postgres
full-text search, filter q/categoryId/status/sellerId) + discover (trang chủ). Category CRUD
đầy đủ.

✅ (2026-10-09) **Product tự chuyển SOLD khi đấu giá kết thúc có người thắng** —
catalog-service trước đây **0 Kafka consumer** (chỉ có producer/outbox), giờ thêm
`AuctionEventsListener` nghe `auction-events`, nhận `AuctionWon` → `MarkProductSoldUseCase`
(system-triggered, không qua ownership check như `ChangeProductStatusUseCase`, idempotent
no-op nếu đã SOLD). Verify sống đầy đủ: tạo product→auction→bid→ép hết giờ→xác nhận
product chuyển ACTIVE→SOLD, biến mất khỏi `/search?status=ACTIVE`. Bump common-libs
1.0.0→1.6.0 (cho `AuctionWonEvent`). Commit `93ce675`.

⚠️ `GET /discover` chỉ lọc `status=ACTIVE`, chưa có logic "phổ biến"/"sắp hết giờ đấu giá" như
SRS mô tả.

⚠️ **Chưa xử lý case bid-and-run**: nếu người thắng không thanh toán (24h timeout), product
vẫn đứng yên ở SOLD vĩnh viễn — không có relist/revert vì chưa làm cơ chế "second-chance"
(xem mục 6, gap #2 của audit "luồng mua hàng đấu giá").

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

✅ (2026-10-10) **Giữ hàng thật qua fulfillment-service** (xem mục 4, Fulfillment Service)
— order được tạo trước, hết hàng thì tự huỷ sau vài giây (bất đồng bộ qua Kafka, không
block checkout). Đây KHÔNG phải "re-validate giá/tồn kho TRƯỚC khi tạo order" (cart vẫn
chưa làm) — là validate SAU, async.

⚠️/❌ **Không có refund thật** (huỷ order đã thanh toán chỉ là out-of-scope có chủ đích, chưa
code), **không có invoice/receipt**, **không có Admin xem tất cả order** (chỉ xem order của
chính mình), cart vẫn không re-validate giá/tồn kho NGAY lúc thêm vào giỏ/trước khi tạo
order, chưa có cart-expiration job. (Bug route gateway đã fix 2026-10-09, xem mục 6.)

### notification-service (port 8084)
✅ Tự động ghi log mọi event nghe được từ Kafka (`auction-events`/`catalog-events`/
`user-events`... — **cần thêm nghe `commerce-events` nếu chưa có, chưa verify**), xem lịch sử
thông báo của mình, Admin xem toàn bộ (audit).

✅ (2026-10-09) **Gửi email thật (SMTP)** — mỗi notification resolve được (UserRegistered/
BidPlaced/Outbid/AuctionWon/AuctionSettled/PasswordResetRequested) giờ cũng được gửi email
qua `SmtpEmailSenderAdapter` (`JavaMailSender`, cấu hình qua `MAIL_HOST/PORT/USERNAME/
PASSWORD`). Email người nhận lấy từ cache local `known_user_emails` (populate từ
`UserRegisteredEvent` — service này **không** gọi sync sang user-service để lấy email,
đúng nguyên tắc polyrepo). **Chưa có tài khoản SMTP thật trên máy này** — khi
`MAIL_USERNAME` rỗng, adapter chỉ log "would have sent" thay vì thử kết nối; verify sống
bằng cách đọc log này (xem commit `2ef9fd7`). Khi có SMTP credentials thật, chỉ cần set
3 biến môi trường, không cần sửa code.

❌ **Vẫn thiếu**: push notification thật (chỉ email). Không có preference người dùng.
Không có retry/delivery-status khi gửi email thất bại (log lỗi rồi bỏ qua, fail-soft).
Không có health-check cho message broker (SRS yêu cầu). Không có springdoc/Swagger (các
service khác đều có).

### Fulfillment Service (port 8086, repo mới `fulfillment-service`, DB `fulfillment_db` 5437)
✅ (2026-10-10) **Inventory core (SRS 3.6.1), 1 warehouse cố định ("MAIN", SRS cho phép ở
MVP)** — `inventory_records` (sku_id+warehouse_id, total/reserved/unavailable_quantity,
available derived), `inventory_reservations` (reference_id unique = idempotency key,
PENDING/COMMITTED/RELEASED, TTL 15 phút), `inventory_ledger` (bất biến, ghi mọi movement
INTAKE/RESERVE/RELEASE/COMMIT). 4 use case: intake/reserve/commit/release + get, cộng
`ExpireReservationsJob` tự release reservation quá hạn (tái dùng use case release, không
rule riêng). **Atomicity**: reserve/release/commit dùng conditional `UPDATE ... WHERE
available >= quantity` trực tiếp ở DB (`@Modifying` JPQL, không load-mutate-save), intake
dùng `INSERT ... ON CONFLICT DO UPDATE` native query. 45 test, gồm 1 test concurrency thật
(20 thread tranh 5 suất qua Testcontainers Postgres thật, không mock) chứng minh không
oversell. Verify sống đầy đủ qua Docker: intake→reserve→insufficient-stock(409)→idempotent
retry→commit→reserve lại→release, ledger ghi đủ audit trail. Commit `891fa9e`
(fulfillment-service) + `565c8c9` (infra, wire docker-compose).

✅ (2026-10-10) **Nối vào checkout thật của commerce-service, theo hướng bất đồng bộ**
(user chốt: giữ đúng nguyên tắc "không service nào gọi sync service khác" của dự án, chấp
nhận khoảng tạo-order-rồi-huỷ ngắn nếu hết hàng). common-libs 1.8.0 thêm 3 event:
`InventoryReservationRequestedEvent` (commerce→fulfillment, publish ngay sau khi tạo order
ở CẢ 2 nơi: `StartCheckoutUseCase` và `CreateOrderFromAuctionUseCase`) +
`InventoryReservedEvent`/`InventoryReservationFailedEvent` (fulfillment→commerce, topic mới
`fulfillment-events`). fulfillment-service được thêm outbox+Kafka (trước đó hoàn toàn
standalone, không có cả 2). `ReserveForOrderUseCase` giữ hàng cả order trong **1
transaction** — item nào thiếu hàng thì exception lan ra làm Spring tự rollback TOÀN BỘ
(kể cả các item đã giữ được trước đó trong vòng lặp), không cần code compensate/release
tay. commerce-service nhận `InventoryReservationFailed` → tự huỷ order
(`CancelOrderDueToStockFailureUseCase`, system-triggered, publish `OrderCancelledEvent` y
hệt đường buyer-cancel để auction-service vẫn xử lý thống nhất). `OrderPaid`/`OrderCancelled`
→ fulfillment-service tự commit/release toàn bộ reservation của order đó (tìm qua tiền tố
`referenceId = "<orderId>:<skuId>"`, không cần thêm cột DB). **Giới hạn đã biết**:
`productId` được dùng làm `skuId` luôn (vì hiện mỗi product chỉ có đúng 1 SKU — nếu sau
này 1 product có nhiều SKU thì chỗ này phải sửa). Verify sống đủ cả 3 luồng qua Docker:
(1) checkout đủ hàng → reserve → huỷ → release; (2) checkout thiếu hàng → order tự
`AWAITING_PAYMENT → CANCELLED` sau vài giây, tồn kho không đổi (nhờ rollback transaction).
59 test (fulfillment-service) + 67 test (commerce-service), tất cả pass. Commit `5abc498`
(fulfillment-service) + `d451231` (commerce-service) + `999a992` (infra).

⬜ **Chưa làm**: Shipping & Logistics (SRS 3.6.2). Privilege gating riêng cho
reserve/commit/release (hiện chỉ yêu cầu authenticated() — vì caller thật là
commerce-service qua Kafka, không phải người dùng cuối, nên privilege theo kiểu
`@RequiresPrivilege` chưa thật sự cần thiết; endpoint REST hiện tại chủ yếu để test/demo
thủ công).

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

1. ✅ (2026-10-09) Sửa route `api-gateway` cho commerce-service — thêm route
   `/api/v1/carts/**,/api/v1/checkout,/api/v1/orders/**` → `lb://commerce-service`. Verify
   sống qua `:8080` (login + GET carts/me + POST checkout đều route đúng).
2. ✅ (2026-10-09) Admin User CRUD + Role management ở user-service — commit `94a6cc9`,
   pushed GitHub + synced GitLab monorepo. Chi tiết xem mục 4 (user-service).
3. ✅ (2026-10-09) Logout + Forget/Reset Password ở user-service — commit `f9a8fc3` +
   common-libs 1.5.0 (`efbc581`), pushed GitHub + synced GitLab monorepo. Chi tiết xem mục 4.
4. Phần lớn hơn, làm tới đâu tính tới đó (KHÔNG tự ý làm hết 1 lượt):
   - ✅ (2026-10-09) Notification gửi email thật (SMTP) — xem mục 4 (notification-service),
     commit `2ef9fd7` (notification-service) + `8b6a2f1` (user-service, publish
     `PasswordResetRequestedEvent`) + common-libs 1.6.0 (`45f3d4a`). Pushed GitHub + synced
     GitLab.
   - ✅ (2026-10-09) Audit riêng "luồng mua hàng đấu giá đã ổn chưa" (user yêu cầu) — xác
     nhận happy path (Bid→AuctionWon→order→Stripe→OrderPaid→MarkAuctionPaid) đúng, có
     idempotency tốt; `MAX_ACTIVE_AUCTIONS_PER_SELLER` thực ra ĐÃ enforce đúng (ghi chú cũ ở
     mục 4 nói "cần xác minh lại" là sai, đã sửa). Tìm ra 3 gap, đã fix gap #1 (xem
     catalog-service ở trên). Gap #2 và #3 lúc đó CHƯA làm.
   - ✅ (2026-10-09) **Gap #3 đã fix**: `OrderCancelledEvent` giờ mang thêm `auctionId`
     (null cho order mua trực tiếp); `CommerceEventsListener` bên auction-service xử lý
     thêm `OrderCancelled` → gọi ngay `EmitPaymentTimeoutUseCase.emit()` (dùng lại y
     nguyên use case idempotent mà `PaymentDeadlineJob` dùng, không phát sinh rule mới) —
     buyer huỷ order trước hạn thanh toán giờ bị trừ điểm uy tín **ngay lập tức**, không
     cần chờ 24h. Verify sống đầy đủ: huỷ order → điểm uy tín giảm 50→40
     (TRUSTED→NORMAL) trong vài giây. **Phát hiện và fix thêm 1 bug có từ trước** trong lúc
     verify sống: `CancelOrderUseCase.cancel()` thiếu `@Transactional` nên
     `findByIdForUpdate` (dùng `PESSIMISTIC_WRITE` lock) ném `TransactionRequiredException`
     ở MỌI lần gọi thật — bị che khuất vì test duy nhất của use case này mock repository,
     không chạm lock thật (giống đúng kiểu bug LazyInitializationException đã gặp trước đó
     trong dự án). Bump common-libs 1.6.0→1.7.0. Commit `c8c4ceb` (commerce-service) +
     `32f6940` (auction-service).
   - ⬜ Gap #2: không có relist/second-chance khi người thắng bùng kèo (auction kết thúc
     vĩnh viễn ở ENDED, hàng "mất trắng", chỉ người bùng kèo bị trừ điểm uy tín). Chưa làm
     vì SRS không yêu cầu cụ thể cơ chế này — cần thống nhất với user trước khi tự chế
     thêm rule.
   - ⬜ Push notification thật.
   - ⬜ Refund thật, invoice.
   - ✅ (2026-10-10) Fulfillment Service — Inventory core + đã nối vào checkout thật của
     commerce-service (bất đồng bộ qua Kafka), xem mục 4 (Fulfillment Service) để biết
     chi tiết.

*(Đánh dấu ✅ khi xong, cập nhật ngày + tóm tắt ngắn ở đây thay vì để trạng thái cũ.)*

## 7. Đọc lại SRS gốc nếu cần

File `.docx` không đọc trực tiếp được (máy này thiếu `pandoc`/`soffice`). Cách đã dùng:
`unzip -oq "C:\FPT\srs-nexus-ecommerce-auction-v1.docx" -d <scratchpad>/srs_unpacked/`, rồi
chạy script Python strip tag XML trên `word/document.xml` → text thuần (~59000 ký tự). Toàn bộ
nội dung SRS (mục 2.7 bảng privilege, mục 3.1-3.6 requirement chi tiết, mục 5 NFR) đã được đọc
và đối chiếu với code thật ít nhất 1 lần (2026-10-09) — xem mục 4 ở trên là kết quả, không cần
đọc lại SRS từ đầu trừ khi cần tra câu chữ chính xác.
