# Project Nexus — Handoff / Trạng thái hiện tại

**Cập nhật lần cuối:** 2026-10-28. File này tồn tại để 1 session Claude Code mới đọc xong là
nắm được toàn bộ trạng thái dự án **mà không cần đọc lại từng file code** — tốn ít token hơn
nhiều so với tự khám phá lại từ đầu. Khi có thay đổi lớn (thêm service, đổi kiến trúc, fix bug
quan trọng), **cập nhật file này** thay vì để nó lỗi thời như lần trước (bản cũ dừng ở lúc
auction-service còn chưa viết dòng code nào, trong khi thực tế đã xong rất nhiều).

## 0. Đọc trước: phiên làm trên MacBook (2026-10-17 → 19)

Mọi thứ dưới đây **đã commit + push lên GitHub, CI xanh** (chi tiết từng việc ở mục 4). Chưa sync
sang GitLab monorepo (mục 1). Máy Windows cần: `git pull` cả 11 repo + `mac-handoff`, rồi **build
lại toàn bộ** (mục 8) vì 5 service và `common-libs` (giờ **1.14.0**) đều đổi.

**Đã làm:**
1. **CI chạy thật cho 7 service.** api-gateway/catalog/auction đỏ từ 8–9/10 mà không ai biết:
   Mockito 5 không tự gắn agent được trên runner GitHub ("Could not self-attach") → mọi test
   Mockito lỗi. Sửa: surefire nạp `byte-buddy-agent` làm `-javaagent` (pom của 7 service). Thêm
   `ci.yml` cho notification/commerce/fulfillment (trước chưa có CI). Retro Sprint 0 ghi "chưa có
   CI" — giờ đã có.
2. **Lỗ hổng giá giỏ hàng (2a):** trước đó gọi API với `unitPrice: 1` là mua được giá 1đ, `sellerId`
   giả làm đơn hiện ở seller khác. Giờ commerce giữ bản sao sản phẩm (`product_snapshots`) từ
   `catalog-events`, giá/tên/người bán lấy từ đó; checkout tính lại giá. Mã mới
   `PRODUCT_NOT_AVAILABLE`, `CART_ITEM_UNAVAILABLE`.
3. **Kiểm tồn kho khi thêm giỏ/sửa số lượng/checkout (2b):** fulfillment phát
   `InventoryLevelChanged`, commerce giữ `product_stock_levels`. Mã mới `INSUFFICIENT_STOCK`.
4. **Đấu giá đồ của người khác (2c):** trước đó seller B tạo được phiên cho sản phẩm của A (tiền
   về B, kho của A bị trừ). Giờ auction giữ bản sao chủ/trạng thái/kho, từ chối với
   `NOT_THE_PRODUCT_OWNER` (403), `PRODUCT_NOT_AVAILABLE`, `INSUFFICIENT_STOCK`.
5. **Giữ 1 cái suốt phiên đấu giá + đưa sản phẩm về bán lại khi người thắng bỏ cọc** (gap #2,
   chọn cách đơn giản: SOLD → ACTIVE, seller tự quyết bán tiếp/đấu giá lại).
6. **Sửa lỗi có từ trước: hàng giữ hết hạn sau 15 phút** trong khi đơn chờ trả tới 30 phút (đấu
   giá 24h) → trả tiền muộn thì kho không bị trừ (bán vượt). Đơn thường giờ giữ 35 phút.
7. **Kiểm thử chạy thật mới:** `smoke_auction.py` (đấu giá → thanh toán, bỏ cọc, đơn quá hạn 30
   phút) trong `mac-handoff/scripts`. Lần chạy cuối 2026-10-18: `smoke.py` 26/26 + 10/10,
   `smoke_auction.py` 33/33 + 3/3, cả hai có trả thẻ Stripe thật.
8. **Sửa ghi chú bẫy MapStruct** (mục 3, 9): lỗi "No qualifying bean ...ApiMapper" là do **extension
   Java của IDE tự build đè `target/classes`**, không phải do Windows. Trên Windows vẫn gặp nếu mở
   IDE lúc build: đóng IDE, tắt `java.autobuild.enabled`, hoặc build ở thư mục tạm.

**Lưu ý khi chạy lần đầu trên Windows sau khi pull:**
- `common-libs` 1.14.0 đã phát hành lên GitHub Packages; `build-all.sh` cũng tự `mvn install` nó.
- `infra/.env` (khoá Stripe `sk_test_...`) **không nằm trong git**; máy Windows có sẵn file cũ thì
  dùng tiếp, khoá trên Mac lấy ở Stripe Dashboard → Sandbox → Developers → API keys.
- Lần đầu khởi động bản mới, **auction đọc lại toàn bộ lịch sử** `catalog-events` +
  `fulfillment-events` (an toàn, chỉ tạo bản sao; có thể sinh bản sao cho sản phẩm thử đã xoá — vô
  hại). Riêng listener `auction-events` của fulfillment đọc từ `latest` nên **không** giữ hàng cho
  phiên cũ. Phiên tạo trước bản sửa không có hàng giữ: thắng + trả tiền thì kho không bị trừ.
- Sản phẩm tạo trước 17/10 không có bản sao trong commerce → **không thêm vào giỏ được**; tạo sản
  phẩm mới để thử. Sau khi bật ACTIVE/nhập kho, chờ vài giây mới thêm giỏ/tạo phiên được.
- FE (`nexus-frontend`) **chưa** theo kịp: body thêm giỏ chỉ còn `productId` + `quantity`, các mã lỗi
  mới ở trên, form địa chỉ, `initialStock`, nút nhập kho. `API.md` đã cập nhật đủ.

**Việc tiếp theo đã bàn (chưa làm):** chốt A/B cho Sprint 1 (hạn 21/10) + hỏi tiến độ Khoa/Mai Anh
(xem `mac-handoff/HANDOFF-PRIVATE.md`); FE cho các tính năng trên; dispute; push notification.

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

**GitLab có 2 nhánh với 2 vai trò khác nhau** (từ 2026-10-07): `main` = bản đầy đủ do leader
code solo (đồng bộ từ GitHub bằng subtree như trên); `develop` = **bản khung** (cấu trúc
hexagonal + file cấu hình, chưa có logic nghiệp vụ) để thành viên trong nhóm tự viết, mọi
merge request của họ đi vào `develop`. Hai nhánh KHÔNG đồng bộ với nhau: `catalog-service`
ở `develop` là bản các bạn tự viết (có thể khác `main`, ví dụ chưa có `initialStock`), và
`nexus-frontend/API.md` ở `develop` có chú thích "chưa có ở nhánh develop" riêng, nên khi sync
`API.md` sang `develop` chỉ lấy lại mục tương ứng, đừng ghi đè cả file. Máy chủ GitLab của
trường báo bảo trì 21:00–22:00 hằng ngày đến hết 2026-10-18, tránh push trong khung giờ đó.

## 2. Các repo (clone làm sibling trong `C:\FPT`)

```
C:\FPT\
  common-libs/        # 4 module dùng chung, version hiện tại 1.11.0
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
- **⚠️ MapStruct impl thiếu `implements` → "No qualifying bean of type ...ApiMapper"**:
  nguyên nhân (xác định 2026-10-17 trên MacBook) là **extension Java của IDE** (Red Hat Java /
  JDT trong VS Code, Antigravity...) tự build vào `target/classes` bằng trình biên dịch Eclipse,
  không chạy MapStruct, ghi đè `*MapperImpl.class` của Maven bằng bản thiếu `implements` (dấu
  hiệu: `javap -v` thấy thuộc tính `InconsistentHierarchy`, chỉ Eclipse compiler sinh ra). Xảy
  ra **ngẫu nhiên** (tranh nhau với IDE), ở mọi service IDE đang mở (đã gặp ở catalog-service và
  user-service, cả khi build jar lẫn khi `mvn test`). Kiểm tra nhanh: `javap -p -cp target/classes
  <...>MapperImpl` phải thấy `implements`. Cách tránh: đóng IDE khi build/test, hoặc tắt tự build
  (`"java.autobuild.enabled": false`), hoặc build ở thư mục tạm (mục 9). CI không bị vì không có
  IDE. (Ghi chú 2026-10-17 trước đó đổ cho bước biên dịch test là **sai**: chỉ trùng hợp.)

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

✅ (2026-10-10) **Admin điều chỉnh thủ công điểm uy tín** — đúng privilege code SRS định
nghĩa sẵn (`USER.REPUTATION.ADJUST`, chỉ Admin). `ReputationProfile.adjust(delta)` (delta
có dấu, khác `applyPenalty` luôn trừ) + bảng audit riêng `reputation_adjustments` (khác
`reputation_penalties` — penalty luôn là hệ quả tự động cố định, adjustment là override tay
có lý do + ghi ai làm). Verify sống: +15 điểm đưa user từ NORMAL(40)→TRUSTED(55), non-admin
bị 403 đúng. Migration V13. Commit `a21b835`.

❌ **Vẫn thiếu**: Dispute handling (SRS có nhắc) — chưa có gì.

✅ (2026-10-13→16) **Privilege mới cho luồng bán hàng**, đều không có trong bảng privilege gốc
của SRS: `ORDER.MARK_SHIPPED` (V14, Seller + Admin), `INVENTORY.MANAGE`/`INVENTORY.VIEW`
(V15: MANAGE chỉ Admin; VIEW cho Admin/Seller/Support Staff), `INVENTORY.RESTOCK` (V16, Seller
+ Admin; quyền sở hữu do fulfillment-service tự kiểm, privilege này một mình không cho đụng
kho người khác). Muốn token có privilege mới phải login lại.

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

✅ (2026-10-16) **`initialStock` khi tạo sản phẩm; `ProductCreatedEvent` mang `sellerId` và
`initialStock`** (common-libs 1.11.0; constructor 3 tham số cũ và event cũ vẫn đọc được).
Để fulfillment-service tự mở tồn kho + ghi chủ kho từ sự kiện, không gọi sync sang catalog.
`CreateProductRequest.initialStock` (tuỳ chọn, 0–1.000.000, mặc định 0). catalog-service
nâng common-libs 1.6.0→1.11.0.

⚠️ **Hai mã dễ nhầm**: `Sku.create` sinh `skuId` ngẫu nhiên, KHÁC `productId`. Response sản
phẩm trả cả `id` (mã sản phẩm) lẫn `skuId`; tồn kho và mua hàng luôn tính theo **`id`/
productId** (xem mục fulfillment bên dưới). Nhập kho theo `skuId` là sai và đơn sẽ báo hết
hàng dù kho có hàng.

### auction-service (port 8083)
✅ Đầy đủ nhất trong toàn hệ thống: tạo/sửa/huỷ đấu giá, lifecycle tự động
(`AuctionLifecycleJob`, PENDING→ACTIVE→ENDED), đặt giá với lock chống race condition
(`SELECT...FOR UPDATE`), anti-sniping (tự gia hạn), xác định người thắng, payment deadline
(`PaymentDeadlineJob`, 24h, tự phát `AuctionPaymentTimeout` nếu quá hạn — **đã fix bug: không
còn phạt nhầm auction đã thanh toán**), **enforce `trustLevel` thật** (LOW không đặt giá được,
dưới TRUSTED không tạo đấu giá được được — đọc trực tiếp từ JWT, không gọi user-service).
Payment đã chuyển hẳn sang commerce-service xử lý (auction-service chỉ phát event, không tự
tạo order/gọi Stripe nữa).

✅ (2026-10-17) **Tạo phiên chỉ cho sản phẩm của chính seller, đang ACTIVE, còn kho** (đợt 2c).
Trước đó không kiểm gì: seller B tạo được phiên cho đồng hồ của seller A, đơn người thắng ghi tên
B (tiền, địa chỉ, quyền "đã giao") nhưng trừ kho + đánh SOLD sản phẩm của A. auction giữ bản sao
riêng như commerce: `product_snapshots` (V6, chủ + trạng thái, từ `catalog-events`) và
`product_stock_levels` (V7, từ `InventoryLevelChanged` trên `fulfillment-events`, chỉ ghi sự kiện
mới hơn). `CreateAuctionUseCase` (sau kiểm uy tín + thời gian): không có bản sao / không ACTIVE →
409 `PRODUCT_NOT_AVAILABLE`, khác chủ → 403 `NOT_THE_PRODUCT_OWNER`, kho < 1 → 409
`INSUFFICIENT_STOCK`. Group Kafka `auto-offset-reset: earliest` nên lần đầu chạy auction đọc lại
toàn bộ lịch sử 2 topic (an toàn: chèn-nếu-chưa-có / chỉ-nhận-mới-hơn); sản phẩm tạo từ 16/10 (sự
kiện có `sellerId`) có bản sao luôn. Verify sống 8/8 (B đấu giá đồ A → 403, DRAFT/không tồn tại →
409, kho 0 → 409, nhập 1 → tạo được, chủ tạo đúng → 201), `smoke_auction.py` 30/30, `smoke.py` 26/26.
✅ (2026-10-18) **Giữ 1 cái suốt phiên + đưa sản phẩm về bán lại khi người thắng bỏ cọc.**
fulfillment `AuctionEventsListener` (nghe `auction-events` với `auto.offset.reset=latest` để KHÔNG
giữ hàng cho các phiên cũ khi đọc lần đầu): `AuctionCreated` → `HoldForAuctionUseCase` giữ 1 cái
mã `auction:<id>`, hạn 10 ngày (không `@Transactional` có chủ ý: giữ thất bại rollback riêng rồi mới
ghi sự kiện) → thiếu hàng thì phát `AuctionHoldFailed` (common-libs 1.14.0) → auction
`CancelAuctionForMissingStockUseCase` huỷ phiên PENDING/ACTIVE (`cancelledBy SYSTEM`).
`AuctionFailed`/`AuctionCancelled`/`AuctionPaymentTimeout` → nhả. commerce
`CreateOrderFromAuctionUseCase` **thôi xin giữ hàng** (đã có hàng giữ của phiên); `OrderPaid` có
`auctionId` → fulfillment chốt hàng giữ của phiên; `OrderCancelled` có `auctionId` → nhả.
`AuctionPaymentTimeoutEvent` mang thêm `productId` → catalog `RelistProductUseCase` SOLD → ACTIVE
(chỉ từ SOLD) + commerce `CancelDefaultedAuctionOrderUseCase` huỷ đơn còn treo (chặn trả muộn sau
24h). Verify sống 21/21 (giữ khi tạo, không mua thẳng được, không ai đặt giá/huỷ → nhả, thắng →
không giữ cái thứ 2, bỏ cọc → nhả + ACTIVE + tạo phiên mới được, giữ thất bại → tự huỷ phiên, đơn
thường giữ 35 phút); `smoke_auction.py` 33/33 + 3/3 (trả tiền → hàng giữ `auction:` COMMITTED, chỉ 1
hàng giữ), `smoke.py` 26/26 + 10/10. **Phiên tạo trước bản sửa không có hàng giữ**: nếu thắng và trả
tiền thì kho không bị trừ (log cảnh báo ở `CommitAuctionHoldUseCase`).

✅ (2026-10-18) **Sửa lỗi hàng giữ hết hạn sau 15 phút** (có từ trước): mọi hàng giữ cố định 15 phút
rồi `ExpireReservationsJob` tự nhả, trong khi đơn thường chờ trả tới 30 phút và đơn đấu giá tới 24h;
trả tiền sau phút 15 thì bước chốt bỏ qua hàng đã nhả → **kho không bị trừ** (bán vượt). Giờ hạn
theo từng lần giữ: đơn thường 35 phút (`Reservation.DEFAULT_HOLD`, dài hơn mốc tự huỷ 30 phút của
commerce), hàng giữ cho phiên đấu giá 10 ngày.

✅ (2026-10-28) **CHECK ở DB** cho `auctions` và `bids` (V8), xem mục 6 (đợt dọn nhỏ).

❌ Chưa có: auction riêng tư (visibility public/restricted). (`MAX_ACTIVE_AUCTIONS_PER_SELLER` đã
xác minh là có enforce, xem mục 6.)

### commerce-service (port 8085)
✅ Giỏ hàng CRUD, checkout (giỏ→order), tạo order tự động từ `AuctionWonEvent` (Kafka), thanh
toán Stripe thật (checkout session + confirm, idempotent), huỷ order (chỉ khi chưa thanh toán).

✅ (2026-10-10) **Giữ hàng thật qua fulfillment-service** (xem mục 4, Fulfillment Service)
— order được tạo trước, hết hàng thì tự huỷ sau vài giây (bất đồng bộ qua Kafka, không
block checkout). Đây KHÔNG phải "re-validate giá/tồn kho TRƯỚC khi tạo order" (cart vẫn
chưa làm) — là validate SAU, async.

✅ (2026-10-10) **Admin/Support Staff xem được tất cả order** — `GET /api/v1/orders`
(khác `/orders/me`). Cùng privilege `ORDER.LIST` với Buyer/Seller (đúng SRS: privilege này
cả 4 role đều có), nhưng endpoint check thêm role qua `TokenDetails` — Buyer/Seller giữ
`ORDER.LIST` vẫn bị 403 ở endpoint này vì với họ privilege đó chỉ có nghĩa "xem đơn của
mình" qua `/orders/me`. Verify sống: Admin thấy đủ 8 order từ nhiều buyer khác nhau, Buyer
dùng chung token gọi `/orders` bị `ORDER_LIST_ALL_FORBIDDEN` (403). Commit `9cbab8e`.

✅ (2026-10-11) **Refund tiền thật qua Stripe** — `POST /api/v1/orders/{id}/refund`
(`ORDER.REFUND`, đã đúng scope sẵn trong SRS seed: chỉ Admin/Support Staff). `Order` giờ
lưu `paymentReference` (Stripe PaymentIntent id, chụp lúc `confirm-payment` thành công) —
refund gọi thẳng `com.stripe.model.Refund.create(...)` với id đó. Idempotent (refund lại
order đã REFUNDED → no-op, không gọi lại Stripe); order chưa PAID → 409
`ORDER_NOT_REFUNDABLE`. Migration V4 (`payment_reference` column + status `REFUNDED`).

**Cố ý chỉ xử lý tiền, KHÔNG đụng tồn kho/hàng hoá** — service chưa có shipment tracking
(SRS 3.6.2, Shipping & Logistics, vẫn 0%) nên không thể tự biết đơn đã giao hay chưa để
quyết định có nhập lại kho hay không. Phần "refund hàng" lúc đó đã bàn với user, chủ động
để làm sau — xem mục ngay dưới đây, đã làm xong.

✅ (2026-10-11) **Refund hàng (goods) — bản tối giản, không carrier/tracking thật** — làm
tiếp ngay sau refund tiền, theo đúng quyết định "refund tiền trước, refund hàng sau" của
user. SRS 3.6.2 (Shipping & Logistics đầy đủ: carrier adapter, shipping quote, multi-carrier,
circuit breaker...) được xác định là quá lớn cho scope capstone — brainstorm với user chốt
bản tối giản:
- `Order` thêm `shippedAt` (migration V5). `POST /orders/{id}/mark-shipped` — chỉ Seller sở
  hữu item trong order mới gọi được, privilege mới `ORDER.MARK_SHIPPED` (seed ở user-service
  V14 cho Seller + Admin — **không có trong bảng privilege gốc SRS**, đã tra lại xác nhận
  không có code nào tái dùng được, vì shipping chưa từng được scope cho v1.0). `GET
  /orders/selling` — Seller xem order chứa sản phẩm của mình, tái dùng đúng privilege
  `ORDER.LIST` (giống cách `/orders/me` và `/orders` đã làm — cùng privilege, khác endpoint
  theo role).
- `RefundOrderUseCase` giờ rẽ nhánh theo `shippedAt`: **chưa giao** → tự động restock ngay
  (publish `OrderReturnReceivedEvent`, fulfillment-service nghe và gọi lại
  `IntakeInventoryUseCase` có sẵn). **Lưu ý kỹ thuật quan trọng phát hiện giữa chừng**: ban
  đầu định dùng lại `ReleaseReservationUseCase` (như `OrderCancelled`), nhưng reservation của
  1 order đã PAID đã ở trạng thái COMMITTED (commit() trừ thẳng `total_quantity`, không chỉ
  `reserved_quantity`) — `release()` chỉ tác dụng với reservation còn PENDING nên sẽ no-op
  im lặng, hàng **không** thực sự về kho. Phải dùng intake (nhập kho mới) thay vì release.
  Bắt được lỗi này qua live-verify trước khi báo xong, y hệt kiểu bug
  LazyInitializationException/thiếu `@Transactional` đã gặp trước đó trong dự án.
- **Đã giao** → vẫn chỉ refund tiền (giữ nguyên hành vi cũ), không tự đụng kho.
- Resource mới `ReturnRequest` (migration V6, `return_requests` table) cho case đã giao +
  đã refund mà hàng thật sự trả lại: `POST /orders/{id}/returns` (tạo, chỉ hợp lệ khi order
  đã REFUNDED và đã shipped) → `POST /returns/{id}/receive` (xác nhận đã nhận lại hàng →
  publish `OrderReturnReceivedEvent`, restock, idempotent) hoặc `POST /returns/{id}/reject`
  (từ chối, không restock). Cả 3 endpoint tái dùng privilege `ORDER.REFUND` có sẵn
  (Admin/Support Staff) — không cần privilege mới.
- common-libs 1.10.0: `OrderRefundedEvent` thêm field `shipped` (chỉ để tham khảo/audit, fulfillment-service
  KHÔNG dùng để quyết định restock nữa — xem lỗi kỹ thuật ở trên), event mới
  `OrderReturnReceivedEvent(orderId, items)` dùng chung cho cả 2 trigger (refund-trước-khi-
  giao và return-đã-nhận).
- Route gateway thiếu `/api/v1/returns/**` → 404, phát hiện và fix lúc verify sống (thêm vào
  predicate route commerce-service).
- Verify sống đầy đủ qua Docker + Stripe test mode thật: mua→trả tiền→refund trước khi giao
  → tồn kho tự tăng lại ngay (8→9); mua→trả tiền→seller mark-shipped→refund→tồn kho **giữ
  nguyên** (không tự restock); tạo ReturnRequest cho order đã refund+shipped→admin receive→
  tồn kho tăng lại (8→9), gọi receive lần 2 vẫn idempotent không tăng thêm; reject→không đổi
  tồn kho; Buyer gọi mark-shipped/tạo return đều bị 403 đúng; tạo return cho order chưa giao
  bị 409 `RETURN_NOT_APPLICABLE` đúng. Commit `cd7f5c2` (commerce-service) + `a781f34`
  (fulfillment-service) + `527d80c` (user-service) + `eea8f22` (api-gateway, fix route).

Verify sống đầy đủ qua Docker thật + Stripe test mode qua trình duyệt (thẻ test
4242 4242 4242 4242): tạo product → intake tồn kho → buyer mua → thanh toán Stripe thật
(500.000đ, đủ ngưỡng tối thiểu Stripe) → `paymentReference` lưu đúng → Buyer gọi refund bị
403 (thiếu privilege) → Admin refund 200 → status REFUNDED → gọi lại Stripe refund API xác
nhận charge đã refund thật (`charge_already_refunded`) → Admin refund lần 2 vẫn 200
(idempotent, không lỗi). Commit `19022e5`.

✅ (2026-10-13→16) **Các tính năng thêm sau refund hàng**:
- **Hoá đơn** `GET /orders/{id}/invoice`: suy ra từ đơn đã PAID/REFUNDED, không lưu bảng,
  không migration; số hoá đơn `INV-<năm thanh toán>-<8 ký tự đầu orderId>`.
- **Giỏ hàng hết hạn**: `ExpireInactiveCartsJob` xoá giỏ không hoạt động quá 30 ngày
  (`nexus.cart.max-inactive-days`).
- **Tự huỷ đơn mua trực tiếp chưa thanh toán sau 30 phút** (`ExpireUnpaidOrdersJob` quét mỗi
  60s; `nexus.order.unpaid-timeout-minutes`, `nexus.order.unpaid-expiry-check-interval-ms`).
  Chỉ đơn `auctionId = null`: đơn đấu giá có hạn 24h riêng và bị phạt uy tín bên auction, không
  được lẫn. Mỗi đơn được huỷ trong giao dịch riêng (khoá dòng, kiểm tra lại còn
  AWAITING_PAYMENT và còn quá hạn), phát `OrderCancelled` lý do `PAYMENT_TIMEOUT` → kho nhả
  hàng. Phiên Stripe đặt `expires_at` = 30 phút 30 giây (Stripe tối thiểu 30 phút) để thu
  hẹp cửa sổ "trả tiền cho đơn đã huỷ"; **khe hở vẫn còn** nếu người mua mở phiên rất muộn.
- **Địa chỉ giao hàng** (migration V7, 4 cột `ship_*` cho phép rỗng):
  `PUT /orders/{id}/shipping-address` (BUYER, `CHECKOUT.START`; đơn phải còn
  AWAITING_PAYMENT; dùng cho cả đơn mua trực tiếp lẫn đơn từ đấu giá, vì đơn đấu giá tự tạo
  chưa có địa chỉ). `POST .../checkout-session` trả 409 `SHIPPING_ADDRESS_REQUIRED` khi chưa
  có. Seller thấy địa chỉ trong `/orders/selling`. Số điện thoại 8–15 chữ số (bỏ khoảng
  trắng, dấu chấm, gạch).
- **Quy tắc xem đơn** (`OrderAccessPolicy`, dùng chung cho `GET /orders/{id}` và `/invoice`):
  chỉ buyer, seller có hàng trong đơn, ADMIN/SUPPORT_STAFF; người khác 403. Cần vì đơn giờ
  chứa địa chỉ và số điện thoại. **Bẫy đã gặp**: `Set.of(...).contains(null)` ném
  `NullPointerException`, một token không có claim `role` từng gây 500; `canViewAnyOrder`
  trong `OrderController` đã xử lý null.

✅ (2026-10-16) **Webhook Stripe** `POST /api/v1/payments/webhook` (không cần JWT, xác thực bằng
chữ ký `Stripe-Signature` với khoá `STRIPE_WEBHOOK_SECRET`, để trống thì từ chối mọi lời gọi).
Khách tắt tab sau khi trả tiền thì đơn vẫn `PAID`. Webhook và `confirm-payment` cùng đi qua
`PaidOrderSettler` dưới khoá dòng: đơn `AWAITING_PAYMENT` → `PAID`; đơn đã `CANCELLED` (trả
muộn) → tự hoàn tiền Stripe, đơn → `REFUNDED`, event `OrderRefunded` lý do
`PAID_AFTER_CANCELLATION`; đơn đã `PAID`/`REFUNDED` → bỏ qua. Refund gửi kèm idempotency key
`refund-<paymentIntent>`. Thử trên máy: cài Stripe CLI, `stripe listen --forward-to
localhost:8080/api/v1/payments/webhook`, đặt khoá `whsec_...` nó in ra vào `infra/.env`
(`STRIPE_WEBHOOK_SECRET=...`) rồi dựng lại commerce (nhớ bẫy `STRIPE_SECRET_KEY`, mục 9).

✅ (2026-10-17) **Giá, tên, người bán của món trong giỏ lấy từ server** (trước đó lấy nguyên từ
request: gọi API với `unitPrice: 1` là mua được giá 1đ, `sellerId` giả làm đơn hiện ở seller khác).
commerce giữ bảng `product_snapshots` (migration V8) cập nhật từ `catalog-events` qua
`CatalogEventsListener` → `SyncProductSnapshotUseCase` (ProductCreated chèn nếu chưa có, để sự
kiện giao lại không đưa trạng thái về DRAFT; ProductStatusChanged/ProductUpdated sửa bản đã có).
`ProductCreatedEvent` mang thêm `price` (common-libs 1.12.0, catalog gửi giá). Thêm giỏ: sản phẩm
không có bản sao hoặc không `ACTIVE` → 409 `PRODUCT_NOT_AVAILABLE`. Checkout tính lại giá từng món
theo bản sao (vá cả giỏ cũ có giá giả), món không còn bán → 409 `CART_ITEM_UNAVAILABLE`, giữ giỏ.
Request thêm giỏ chỉ cần `productId` + `quantity` (trường cũ vẫn nhận nhưng bỏ qua). **Đánh đổi đã
biết**: bản sao trễ ~4 giây sau khi seller bật ACTIVE; sản phẩm tạo trước bản sửa không có bản sao
(giá chỉ đặt lúc tạo, không có API đổi giá), chỉ tạo lại sản phẩm mới mua được. Verify sống 9/9 bước
(giá giả 1đ bị bỏ, seller giả bị bỏ, DRAFT/không tồn tại → 409, tắt bán sau khi vào giỏ → checkout
409 + giữ giỏ, giỏ cũ có giá 1đ → đơn tính lại đúng giá); `smoke.py` 27/27, `smoke_auction.py`
giai đoạn 1 30/30.

✅ (2026-10-17) **Kiểm tồn kho khi thêm giỏ / sửa số lượng / checkout** (đợt 2b). fulfillment phát
`InventoryLevelChanged {productId, availableQuantity}` (common-libs 1.13.0) qua
`InventoryLevelNotifier`, gọi trong 4 use case gốc intake/reserve/release/commit (mọi thay đổi kho
khác đều đi qua 4 cái này), cùng transaction với thay đổi kho nên giữ hàng nhiều món thất bại thì
không phát gì. commerce lưu vào bảng riêng `product_stock_levels` (V9; tách khỏi
`product_snapshots` vì khác topic, có thể đến trước ProductCreated), chỉ ghi khi sự kiện mới hơn
(`as_of` = `occurredAt`, 1 câu `INSERT ... ON CONFLICT DO UPDATE ... WHERE as_of <= EXCLUDED.as_of`).
Chưa có dòng kho = còn 0. `StockGuard` dùng chung: (trong giỏ + thêm) / số mới / số lượng lúc
checkout vượt còn bán → 409 `INSUFFICIENT_STOCK`, giữ giỏ. Đây là **báo sớm**, bản sao trễ vài
giây; chốt chặn vẫn là `tryReserve` của fulfillment + tự huỷ đơn khi giữ hàng thất bại. Verify sống
16/16 (5 thêm 6 → 409, thêm đủ 5, nhập thêm → thêm được, sửa vượt → 409, người khác mua làm kho tụt →
checkout 409 + giữ giỏ, huỷ đơn → kho trả lại; bản sao luôn khớp số của fulfillment). `smoke.py`
26/26 (bước "đơn vượt kho tự huỷ" thành "thêm 999 bị 409 ngay"), `smoke_auction.py` 30/30.
`cleanup.py` dọn cả 2 bảng mới.

⚠️/❌ **Vẫn thiếu**: phí vận chuyển và thuế. (Kiểm kho lúc tạo đấu giá: xong ở đợt 2c, xem
auction-service.)

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
Sự kiện giao trùng (Kafka at-least-once) không tạo thêm dòng thông báo **và không gửi lại email**
(từ 2026-10-28; trước đó email vẫn gửi mỗi lần giao).
(2026-10-13→15) **Health-check broker** (`BrokerHealthJob` 15s/lần, `BrokerHealthIndicator`
trên `/actuator/health`; Kafka sập đủ ngưỡng thì tự tạm dừng consumer và ghi
`MessageBrokerUnavailable`, hồi phục thì tiếp tục và ghi `MessageBrokerRecovered`),
**Dozzle** xem log container ở http://localhost:9999 và **Swagger** đã có.

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

✅ (2026-10-11) **Restock theo OrderReturnReceivedEvent** — `IntakeForReturnUseCase` mới,
nghe event `OrderReturnReceived` từ commerce-service (2 trigger: refund trước khi giao, và
Admin xác nhận return sau khi giao — xem mục commerce-service, phần "Refund hàng"), gọi lại
`IntakeInventoryUseCase` có sẵn cho từng line item. **Không** dùng `ReleaseReservationUseCase`
cho việc này — reservation của order đã PAID đã COMMITTED (trừ thẳng `total_quantity`),
release chỉ tác dụng với PENDING nên sẽ no-op im lặng nếu dùng sai. common-libs 1.10.0.
Commit `a781f34`.

✅ (2026-10-14→16) **Chủ kho và người bán tự nhập kho, qua sự kiện**:
- **Quy ước (đã chốt)**: kho khoá theo **`productId`**. Commerce gửi `productId` trong trường
  `skuId` của event và kho lưu vào cột `sku_id`; KHÔNG phải `skuId` riêng của catalog. REST
  của kho đã đổi tên trường `skuId`→`productId` (body và response của `/inventory`,
  `/reservations`); event và DB vẫn tên `skuId`/`sku_id` vì đổi sẽ phá tương thích.
- `CatalogEventsListener` nghe `catalog-events`: `ProductCreated` → `RegisterProductStock
  UseCase` ghi chủ kho (`product_stock_owners`, migration V3, chèn `ON CONFLICT DO NOTHING`
  nên sự kiện giao lại không cộng kho hai lần) và nhập `initialStock`. Xử lý lỗi theo quy
  ước cũ: ghi log rồi bỏ qua, chưa retry/dead-letter, Admin vẫn nhập kho tay được.
- `POST /inventory/{productId}/restock` (`INVENTORY.RESTOCK`): Seller chỉ nhập cho sản phẩm
  của mình (403 `NOT_THE_PRODUCT_OWNER`, kể cả sản phẩm chưa có chủ kho để không lộ id nào
  tồn tại); Admin (giữ `INVENTORY.MANAGE`) nhập mọi sản phẩm. Trả lại tồn kho sau khi nhập.
- Gateway thêm tuyến `/api/v1/inventory/**` → fulfillment-service (`/reservations` cố ý
  KHÔNG mở ra ngoài, commerce chạy nó qua Kafka).
- Sản phẩm tạo trước 2026-10-16 chưa có chủ kho, chỉ Admin nhập kho được.

⬜ **Chưa làm**: Shipping & Logistics (SRS 3.6.2) đầy đủ — carrier adapter, shipping quote,
multi-carrier, circuit breaker... (đã đánh giá là quá lớn cho scope hiện tại, xem mục
commerce-service "Refund hàng" để biết bản tối giản đã làm thay). Privilege gating cho reserve/commit/release đã làm (`INVENTORY.MANAGE`/`VIEW`, user-service V15).

### Hạ tầng
`discovery-server`/`api-gateway`/`common-libs`/`infra` (docker-compose) hoạt động tốt, đã
verify end-to-end nhiều lần qua Docker thật (kể cả thanh toán Stripe thật qua trình duyệt).
API versioning `/api/v1` nhất quán. Swagger có ở mọi service (notification-service thêm 2026-10-15).

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
     thêm rule. Đã thấy khi chạy thật (2026-10-17): người thắng huỷ đơn thì kho có lại 1 suất
     nhưng sản phẩm vẫn kẹt ở `SOLD`, không ai mua hay đấu giá lại được.
   - ⬜ Push notification thật.
   - ✅ (2026-10-10) Fulfillment Service — Inventory core + đã nối vào checkout thật của
     commerce-service (bất đồng bộ qua Kafka), xem mục 4 (Fulfillment Service) để biết
     chi tiết.
   - ✅ (2026-10-10) Admin điều chỉnh thủ công điểm uy tín (`USER.REPUTATION.ADJUST`) —
     xem mục 4 (user-service). Commit `a21b835`.
   - ✅ (2026-10-10) Admin/Support Staff xem tất cả order — xem mục 4 (commerce-service).
     Commit `9cbab8e`.
   - ✅ (2026-10-11) Refund tiền thật qua Stripe — xem mục 4 (commerce-service). Commit
     `19022e5`.
   - ✅ (2026-10-11) Refund hàng (goods) — bản tối giản, xem mục 4 (commerce-service +
     fulfillment-service). Commit `cd7f5c2`/`a781f34`/`527d80c`/`eea8f22`. invoice/receipt
     vẫn chưa làm.
   - ✅ (2026-10-12→16) Health-check broker + Dozzle (notification), giỏ hàng hết hạn, hoá
     đơn, tự huỷ đơn quá hạn + `expires_at` Stripe, địa chỉ giao hàng + quy tắc xem đơn, tồn
     kho (`initialStock`, chủ kho, restock, quy ước `productId`). Chi tiết ở mục 4. Smoke test
     xuyên suốt 36/36 bước đạt (mục 8).
   - ✅ (2026-10-16) Webhook Stripe + tự hoàn tiền khi trả muộn (xem mục 4, commerce-service).
   - ✅ (2026-10-16) Hết phạt oan người thắng đấu giá: `CommerceEventsListener` chỉ phạt khi
     `reason = BUYER_REQUESTED`; lý do khác (hết hàng, ...) gọi `ClosePaymentWithoutPenaltyUseCase`
     (đặt cờ `payment_timeout_emitted` mà không phát event, để `PaymentDeadlineJob` không phạt
     sau 24h).
   - ✅ (2026-10-16, gom lại 2026-10-20) **Retry + dead-letter cho Kafka**, nay nằm trong module
     **`common-kafka`** của common-libs **1.15.0** (auto-configuration, service chỉ cần thêm
     dependency; 6 service đã thêm và xoá bản `KafkaErrorHandlingConfig` riêng). Listener không
     `catch (Exception)` nuốt lỗi. Lỗi → thử lại 3 lần (chờ 1s, 2s, 4s) → vẫn lỗi thì chép record
     sang **DLT riêng của service**: `<topic>.<tên-service>.DLT`, ví dụ
     `commerce-events.fulfillment-service.DLT` (riêng từng service để replay không làm service
     khác xử lý lại). JSON hỏng hoặc `DomainException` → vào DLT ngay. Notification: payload
     không parse được ném `MalformedEventException`.
   - ✅ (2026-10-20) **Xem / chạy lại DLT + báo khi kẹt** (`common-kafka`), có ở mọi service đọc
     Kafka, gọi thẳng cổng service (không qua gateway), cần quyền `SYSTEM.DLT.MANAGE` (ADMIN, user
     V17):
     - `GET :808x/internal/dlt?peek=5`: số event đang chờ mỗi DLT + vài event cũ nhất kèm lý do lỗi.
     - `POST :808x/internal/dlt/replay?max=100`: đưa từng event về **đúng listener của service đó**
       (không publish lại lên topic gốc). Thành công thì ghi nhận (offset của group
       `<service>-dlt-replay`), lỗi lại thì dừng và trả về event nào, lỗi gì; event vẫn nằm trong DLT.
       Sửa nguyên nhân trước rồi mới replay.
     - `POST :808x/internal/dlt/skip?topic=<tên DLT>&count=1` (common-libs **1.16.1**): khi một event
       **không bao giờ xử lý được** (payload hỏng) và làm replay dừng ở đó mãi, lệnh này dời con trỏ
       "đã xử lý" của DLT qua `count` event cũ nhất (tối đa 100) mà không xử lý chúng. Event vẫn
       nằm trong topic DLT, chỉ con trỏ của service này dời đi; nội dung event bị bỏ được ghi vào
       log WARN (`DLT skip: dropping ...`) để truy vết. `topic` phải là DLT của chính service đó,
       sai tên thì 404 `DEAD_LETTER_TOPIC_NOT_FOUND`. Dùng khi đã chắc event đó là rác.
     - Mỗi phút kiểm một lần: còn event chờ thì log WARN (xem trên Dozzle) và metric
       `nexus.kafka.dlt.backlog{topic=...}`. Cố ý **không** làm health DOWN.
     - Test Kafka thật: `notification-service/.../KafkaErrorHandlingIntegrationTest` (lỗi → DLT →
       replay → xử lý đúng 1 lần; replay dừng ở event vẫn lỗi). WebMvc test 401/403:
       `commerce-service/.../DeadLetterControllerTest`.
     - Xem DLT bằng tay: `docker exec infra-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh
       --bootstrap-server localhost:9092 --topic commerce-events.fulfillment-service.DLT
       --from-beginning` (Git Bash trên Windows: thêm `MSYS_NO_PATHCONV=1` phía trước).
   - ✅ (2026-10-17→18) (g) kiểm giá + kho lúc thêm giỏ/checkout, kiểm chủ + kho lúc tạo phiên,
     giữ hàng suốt phiên, gap #2 (bỏ cọc → sản phẩm về ACTIVE). Xem mục 0 và mục 4.
   - ✅ (2026-10-21) **Dọn bảng outbox** ở 5 service có outbox (user, catalog, auction, commerce,
     fulfillment): `OutboxCleanupJob` mỗi giờ xoá dòng **đã gửi** quá 7 ngày
     (`nexus.outbox.retention-days`, `nexus.outbox.cleanup-interval-ms`). Dòng chưa gửi không bao
     giờ bị xoá. Test: `OutboxJpaRepositoryTest` (Postgres thật), `OutboxCleanupJobTest`.
   - ✅ (2026-10-21) **Checkout chống bấm hai lần** (commerce): trước đó `StartCheckoutUseCase` không
     có `@Transactional` và không khoá giỏ, nên 2 request cùng lúc có thể tạo **2 đơn** từ một giỏ, và
     lưu đơn / xoá giỏ / ghi outbox là 3 giao dịch rời nhau. Giờ: cả use case một transaction, khoá
     dòng giỏ (`findByBuyerIdForUpdate`) trước tiên, header tuỳ chọn `Idempotency-Key` (bảng
     `checkout_requests`, V10, unique `(buyer_id, key)`): gửi lại cùng mã trả lại đơn cũ. Mã cũ hơn 24h
     bị `PurgeCheckoutRequestsJob` dọn. Test đồng thời với Postgres + Kafka thật:
     `CheckoutConcurrencyIntegrationTest` (bỏ khoá thì 2/3 test đỏ, đã thử). Chưa bảo vệ: thêm món vào
     giỏ đúng lúc đang checkout (không khoá) — hiếm, có từ trước.
   - ✅ (2026-10-22) **Độ phủ test (JaCoCo)** ở 7 service (user, catalog, auction, commerce,
     fulfillment, notification, api-gateway): plugin `jacoco-maven-plugin` 0.8.12 trong pom, báo cáo
     ở `target/site/jacoco` (html, csv, xml) sau `mvn verify`. CI in bảng độ phủ vào trang kết quả
     của lần chạy (Summary) và lưu báo cáo thành artifact `jacoco-report`. `argLine` của surefire
     là `@{jacocoArgLine} -javaagent:...byte-buddy-agent...` (JaCoCo và agent của Mockito cùng
     chạy); bỏ qua lớp sinh tự động `*MapperImpl` và `*Application`. Chưa đặt ngưỡng bắt buộc
     (chưa chặn CI khi độ phủ thấp). Đo ngày 22/10 (dòng / nhánh):
     user 96,0% / 89,1% (175 test), catalog 93,6% / 96,0% (103), auction 97,0% / 94,9% (145),
     commerce 92,7% / 88,0% (245), fulfillment 94,6% / 91,4% (105), notification 91,4% / 87,0% (55),
     api-gateway 92,1% / 86,7% (14). Tổng 842 test. Lưu ý: độ phủ chỉ cho biết dòng nào được chạy
     qua, không cho biết test có kiểm đúng điều cần kiểm hay không.
   - ✅ (2026-10-23) **Nhập lại mật khẩu kiểm ở server** (user-service): đăng ký cần `confirmPassword`,
     đổi mật khẩu và đặt lại mật khẩu cần `confirmNewPassword`; không khớp (so sánh chính xác, phân biệt
     hoa thường) thì `400 VALIDATION_ERROR` với lỗi gắn vào ô nhập lại. Mật khẩu mới từ 8 đến 72 ký
     tự (72 là giới hạn BCrypt), `email` và `fullName` tối đa 255. Tài khoản cũ có mật khẩu ngắn hơn vẫn
     đăng nhập được (chỉ mật khẩu **mới** bị áp quy tắc). Cài ở `api/validation/PasswordsMatch`
     (ràng buộc cấp lớp, báo lỗi trên ô nhập lại). FE đã gửi `confirmPassword` và đổi quy tắc 6 thành 8
     ký tự; `smoke.py` cũng gửi trường mới. **API đăng ký không có `confirmPassword` giờ bị 400**: client
     nào gọi trực tiếp phải thêm trường này (đã ghi trong API.md mục 2.1 và 2.3).
   - ✅ (2026-10-23) **Chấm điểm chỉ cho giao dịch có thật** (user-service, common-libs **1.17.0**):
     commerce phát sự kiện mới `OrderShipped` (orderId, auctionId nếu từ đấu giá, buyerId, sellerId của
     người bấm "đã giao", shippedAt) trong `MarkOrderShippedUseCase`; user-service nghe
     `commerce-events` (`CommerceEventsListener`) và ghi bảng `ratable_transactions` (V18; đơn từ đấu
     giá ghi hai dòng: ORDER và AUCTION). `POST /ratings` kiểm giao dịch có trong bảng (404
     `TRANSACTION_NOT_FOUND`) và người chấm/người bị chấm đúng là hai bên (403 `NOT_A_PARTICIPANT`).
     Không gọi sang commerce lúc chấm. Chỉ chấm được **sau khi người bán bấm "đã giao"**; đơn nhiều
     người bán chỉ ghi người bán đã bấm (đơn chỉ có một `shippedAt`; sửa tận gốc cần lưu ngày giao
     theo từng món, nên làm cùng trạng thái DELIVERED). Đơn đã giao **trước** khi có tính năng này
     không có sự kiện `OrderShipped` nên chưa chấm được (bảng không tự bù ngược).
     **Gateway:** thêm `/api/v1/ratings/**` vào route của user-service (trước đó gateway trả 404 cho
     API này dù `API.md` ghi cho FE dùng, nên FE chưa chấm điểm được).
   - ✅ (2026-10-24) **Ba việc nhỏ về an toàn dữ liệu:**
     - *Mật khẩu tính theo BYTE* (user-service): BCrypt chỉ dùng 72 byte đầu, mà chữ có dấu tiếng Việt
       chiếm 3 byte, nên `@Size(max=72)` (đếm ký tự) chưa đủ. Thêm `@MaxUtf8Bytes` ở đăng ký, đổi, đặt
       lại, admin đặt mật khẩu; `POST /users` của admin trước đó **không có quy tắc mật khẩu nào**, nay
       cùng quy tắc 8 đến 72.
     - *`successUrl` và `cancelUrl` của phiên thanh toán* (commerce-service): phải nằm trên địa chỉ của
       FE (`nexus.checkout.allowed-redirect-origins`, mặc định `http://localhost:5173` và `:3000`,
       cấu hình thành địa chỉ FE thật khi triển khai; để trống thì từ chối hết). `RedirectUrlPolicy` so
       địa chỉ **đã phân tích** (scheme + host + cổng), nên `http://localhost:5173.evil.com` và
       `http://localhost:5173@evil.com` bị chặn như `javascript:` hay địa chỉ tương đối. Sai thì
       `400 VALIDATION_ERROR` ở ô `successUrl` hoặc `cancelUrl`.
     - *`CHECK` cho `orders.status`* (commerce V11): chỉ nhận AWAITING_PAYMENT, PAID, CANCELLED,
       REFUNDED. Thêm giá trị vào `OrderStatus` mà quên mở rộng ràng buộc thì test
       `everyOrderStatusOfTheEnum_...` đỏ. `auctions.status` đã có CHECK từ 28/10 (xem dưới). Các cột
       `status` khác (`return_requests`, `product_snapshots`...) **chưa** có CHECK, làm tương tự khi cần.
   - ✅ (2026-10-26 → 28) **Đợt dọn nhỏ sau khi rà soát:**
     - *CHECK cho `auctions` và `bids`* (auction-service V8): `status` chỉ nhận PENDING, ACTIVE, ENDED,
       CANCELLED; `end_time > start_time`; `starting_price`, `bid_increment` và `bids.amount` lớn hơn 0.
       Trước đó các luật này chỉ nằm trong Java (enum `AuctionStatus`, `validateWindow`, validate request).
       `AuctionCheckConstraintsTest` (13 test) thử từng giá trị sai. Thêm giá trị vào `AuctionStatus` mà
       quên mở rộng ràng buộc thì INSERT bị DB từ chối.
     - *Không gửi email lần hai khi Kafka giao lại sự kiện* (notification-service): `event_id` UNIQUE vốn
       đã giữ một dòng thông báo, nhưng email vẫn gửi mỗi lần giao. `NotificationRepositoryPort.record`
       giờ trả `true/false` (có ghi mới hay không); bản trùng trả `Resolution.ALREADY_RECORDED` nên bước
       gửi email bỏ qua.
     - *`API.md` khớp với code:* bổ sung các mã lỗi service ném ra nhưng tài liệu chưa có (vai trò, đặt
       giá, sửa/hủy phiên, thông báo, quên mật khẩu, phiên thanh toán không khớp đơn, bảng mã dùng chung);
       sửa luật đặt giá đầu tiên (chỉ cần `>= startingPrice`, không cộng bước giá).
     - *Script kiểm tra mã lỗi:* `python scripts/check_error_codes.py` (trong repo này) quét mã lỗi các
       service ném ra và so với `nexus-frontend/API.md`; thoát mã 1 nếu có mã chưa được ghi. Chạy trước khi
       thêm mã lỗi mới. Chưa gắn vào CI (API.md nằm ở repo khác).
     - *FE (`nexus-frontend`):* thêm giỏ hàng, checkout (`Idempotency-Key`, địa chỉ giao hàng, chuyển sang
       Stripe, trang thành công/hủy có thử lại `confirm-payment`), thông báo lỗi tiếng Việt cho 401/403/hết
       kho, bỏ dữ liệu mẫu giả ở danh sách và trang chủ (lỗi thì hiện nút thử lại), giao diện các trang
       trong đồng bộ với landing. Mới build/lint, **chưa chạy trọn luồng thanh toán trên cụm thật**.
   - ⬜ **Việc còn lại, theo ưu tiên gợi ý**: (c) FE: form địa chỉ, nhập
     `initialStock`, nút nhập thêm kho, body thêm giỏ mới + hiện các mã lỗi mới (mục 0);
     (e) preference thông báo; (f) thống kê cho Seller; dispute; push notification.

*(Đánh dấu ✅ khi xong, cập nhật ngày + tóm tắt ngắn ở đây thay vì để trạng thái cũ.)*

## 7. Đọc lại SRS gốc nếu cần

File `.docx` không đọc trực tiếp được (máy này thiếu `pandoc`/`soffice`). Cách đã dùng:
`unzip -oq "C:\FPT\srs-nexus-ecommerce-auction-v1.docx" -d <scratchpad>/srs_unpacked/`, rồi
chạy script Python strip tag XML trên `word/document.xml` → text thuần (~59000 ký tự). Toàn bộ
nội dung SRS (mục 2.7 bảng privilege, mục 3.1-3.6 requirement chi tiết, mục 5 NFR) đã được đọc
và đối chiếu với code thật ít nhất 1 lần (2026-10-09) — xem mục 4 ở trên là kết quả, không cần
đọc lại SRS từ đầu trừ khi cần tra câu chữ chính xác.

## 8. Chạy cả cụm và smoke test

```bash
cd infra
./build-all.sh              # mvn install common-libs + package các service (gồm fulfillment-service)
docker compose build
docker compose up -d
```
Nếu `build-all.sh` báo không có quyền chạy (`permission denied`), dùng `bash build-all.sh`. Kiểm thử
chạy thật (trong `mac-handoff/scripts`, chạy từ thư mục cha):
`python3 mac-handoff/scripts/smoke.py` → trả thẻ ở link in ra → `... smoke.py after-payment`;
tương tự `smoke_auction.py`; dọn: `python3 mac-handoff/scripts/cleanup.py` (dọn cả các bảng bản
sao mới ở commerce/auction).

- Cần `STRIPE_SECRET_KEY` (khoá **test**, `sk_test_...`) trong môi trường khi chạy
  `docker compose` (hoặc `infra/.env`, **không commit**). Compose đọc `${STRIPE_SECRET_KEY}`
  lúc tạo container: tạo lại `commerce-service` khi biến này rỗng sẽ làm container mất khoá.
  Muốn dựng lại một service mà không đụng các service khác: `docker compose up -d --no-deps
  <tên>`.
- Cổng: gateway 8080, Eureka 8761, user 8081, catalog 8082, auction 8083, notification 8084,
  commerce 8085, fulfillment 8086, Dozzle 9999. Sau khi khởi động lại, gateway có thể trả 500
  vài giây do bộ nhớ đệm Eureka cũ; tự hết.
- **Smoke test xuyên suốt qua gateway** (đã chạy 2026-10-16, 36/36 bước đạt): tạo admin,
  hai seller (qua luồng yêu cầu bán hàng + duyệt), buyer; danh mục; sản phẩm có `initialStock`;
  kho tự mở qua Kafka; nhập thêm đúng chủ/sai chủ/Admin; giữ hàng khi đặt; đơn vượt tồn kho tự
  huỷ; quy tắc xem đơn; địa chỉ giao hàng chặn thanh toán; phiên Stripe; **người thật nhập
  thẻ thử `4242 4242 4242 4242` trên trang Stripe**; xác nhận thanh toán; trừ kho; seller thấy
  địa chỉ; đã giao; hoá đơn; hoàn tiền; trả hàng; nhập lại kho. Bước thẻ không tự động hoá
  được, và sau khi thanh toán Stripe chuyển về `successUrl` (không có FE nào chạy ở đó thì
  trình duyệt báo lỗi kết nối, vô hại). Dữ liệu thử dùng email `smoke-*@nexus.dev`, dọn bằng
  cách xoá theo các email đó ở 5 cơ sở dữ liệu. Kịch bản (`smoke.py`, `cleanup.py`) không
  nằm trong repo, chúng ở gói bàn giao riêng của leader.
- **Kiểm thử đấu giá** (2026-10-17, 33/33 bước đạt, `smoke_auction.py` trong gói bàn giao):
  tạo phiên → ACTIVE, seller không tự đặt giá được, giá thấp bị từ chối, thông báo Outbid, ép hết
  giờ bằng SQL → ENDED đúng người thắng/giá, sản phẩm SOLD, đơn tự tạo + giữ 1 suất, trả Stripe
  → `paidAt` có, kho trừ về 0. Người thắng tự huỷ đơn → trừ uy tín ngay 50→40 (TRUSTED→NORMAL),
  nhả kho. Đơn mua trực tiếp lùi `created_at` 31 phút → job huỷ (`CANCELLED`), nhả kho, không đụng
  uy tín.

## 9. Bẫy đã gặp (đọc trước khi vào việc)

- **Phát hành `common-libs` hỏng giữa chừng thì phải đổi số phiên bản.** `mvn deploy` đẩy từng module theo
  thứ tự; nếu test của một module sau (ví dụ `common-kafka`) đỏ thì các module trước đã nằm trên
  GitHub Packages, và GitHub Packages **không cho ghi đè** một phiên bản (lỗi `409`). Ngày 23/10
  `1.16.0` bị như vậy (chỉ có parent, core, web, events, security, thiếu `common-kafka`), nên dùng
  **`1.16.1`**; đừng dùng `1.16.0`. Trước khi push `common-libs`, chạy `mvn clean install` (có test) ở
  bản sao tạm. Test trong `common-libs` **không được dùng Mockito** (module không có agent
  byte-buddy, Mockito không tự gắn được trên máy chủ CI; ở máy bạn vẫn chạy được nên dễ bị lọt).

- **Build trên Windows với IDE mở**: IDE tự biên dịch vào `target/`, build trực tiếp trong
  thư mục thật dễ gây lỗi `NoClassDefFoundError`/mapper cũ. Cách ổn định: chép service ra thư
  mục tạm (loại `target/`), build và chạy test ở đó, rồi chép jar về `target/` khi cần dựng
  Docker.
- **IDE mở trên máy Mac cũng bị** y như Windows (xem mục 3, bẫy MapStruct): không phải lỗi của
  Windows mà của extension Java tự build vào `target/`. `build-all.sh` dùng
  `-Dmaven.test.skip=true` chỉ để build nhanh hơn, không tránh được lỗi này.
- **Testcontainers 1.19.8 + Docker Engine 29** (Mac): Docker trả 400 vì API cũ → "Could not find
  a valid Docker environment". Sửa cục bộ: `echo api.version=1.44 > ~/.docker-java.properties`.
- **Mockito trên CI**: Mockito 5 tự gắn agent lúc chạy, runner GitHub không cho ("Could not
  self-attach") → mọi test Mockito lỗi. Đã sửa 2026-10-17: surefire nạp `byte-buddy-agent` làm
  `-javaagent` (pom của 7 service). Giả lập trên máy: `JDK_JAVA_OPTIONS=-XX:+DisableAttachMechanism`.
- **Docker Desktop hay tắt** giữa chừng; test dùng Testcontainers sẽ báo "Could not find a
  valid Docker environment". Bật lại rồi chạy lại.
- **Sửa `common-libs`**: bump version trong cả 5 `pom.xml`, `mvn install` để service build
  cục bộ được, rồi push để workflow phát hành lên GitHub Packages (mất khoảng 1 phút rưỡi);
  sau đó mới nâng `common-libs.version` ở các service dùng nó. Tải từ GitHub Packages cần
  token đọc gói (`read:packages`) trong `~/.m2/settings.xml`, server id `github`.
- **Lệnh Maven chạy lâu** (cả bộ test của một service vài phút): chạy nền và chờ thông báo,
  đừng lặp lại.
- **`Set.of(...)`/`Map.of(...)` ném NPE với `contains(null)`**: luôn kiểm tra null trước.
- **Kafka**: chỉ các service nghe topic mới nhận sự kiện; sự kiện lỗi hiện chỉ được ghi log
  (chưa retry).
- **Mọi chỗ dùng `SELECT … FOR UPDATE`** cần `@Transactional` ở use case, nếu không Hibernate
  ném `TransactionRequiredException` (test mock repository sẽ che lỗi này).

## 10. Chuyển sang máy khác (Windows ↔ MacBook)

Mã nguồn đã nằm trên GitHub, không cần mang gì khác ngoài những thứ dưới đây. Mỗi lúc chỉ làm ở
**một** máy; trước khi đổi máy, commit + push hết ở máy đang dùng. Máy đã có sẵn repo thì chỉ cần
`git pull` từng repo (hoặc `bash mac-handoff/setup-mac.sh <thư mục cha>`, nó cũng pull repo đã có)
rồi build lại như mục 8.

1. Cài JDK 21, Maven, Docker Desktop, git và (nếu dùng) `gh`.
2. Clone **11 repo cạnh nhau** trong một thư mục: `infra`, `common-libs`, `discovery-server`,
   `api-gateway`, `user-service`, `catalog-service`, `auction-service`, `notification-service`,
   `commerce-service`, `fulfillment-service`, `nexus-frontend` (đều `github.com/antran19/<tên>`).
3. `~/.m2/settings.xml` với server `github` (tên người dùng + token `read:packages`) để tải
   `common-libs` đã phát hành. Hoặc `mvn install` `common-libs` cục bộ trước.
4. Tạo `infra/.env` có `STRIPE_SECRET_KEY` thử (**không commit**).
5. Build và chạy như mục 8. Mac với Docker Desktop mới (Engine 29): Testcontainers 1.19.8 cần
   `echo api.version=1.44 > ~/.docker-java.properties` (mục 9), không thì test DB báo "Could not
   find a valid Docker environment".
6. Với Claude Code: mở thư mục cha chứa các repo, bảo nó đọc file này đầu tiên; `CLAUDE.md`
   của từng service sẽ được nạp khi làm việc trong thư mục đó. Những thứ **không** nằm trong
   git (hướng dẫn agent review, ghi nhớ cách làm việc cá nhân) phải chép riêng.
