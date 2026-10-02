# Project Nexus — Ghi chú tổng kết buổi học code

File này tổng kết lại toàn bộ buổi đọc code cùng Claude, đi từ nền tảng (`common-libs`)
lên tới hạ tầng chạy chung (`infra`). Không nằm trong git repo nào — chỉ để tham khảo cá nhân.

---

## 1. `common-libs` — 4 module dùng chung

| Module | Chứa gì | Dùng để làm gì |
|---|---|---|
| `common-core` | `ApiResponse<T>`, `DomainException` + 5 lớp con (`NotFoundException`, `ConflictException`, `ForbiddenException`, `UnauthorizedException`, `ValidationException`) | Khung response chuẩn + hệ thống lỗi nghiệp vụ. Use case chỉ `throw`, không tự dựng `ResponseEntity`. |
| `common-web` | `GlobalExceptionHandler` (`@RestControllerAdvice`) | Bắt mọi exception từ `common-core`, tự map sang đúng HTTP status. 1 class dùng chung cho cả 3 service. |
| `common-security` | `JwtAuthenticationFilter`, `JwtTokenProvider`, `@RequiresPrivilege` + `PrivilegeAuthorizationAspect` | Đọc JWT → gán `userId`+quyền vào `SecurityContextHolder`. `@RequiresPrivilege` dùng AOP để chặn method thiếu quyền trước khi nó chạy. **Lưu ý:** `JwtAuthenticationFilter` không load được trong `api-gateway` (WebFlux, không có servlet API) — `@ConditionalOnClass(jakarta.servlet.Filter)`. |
| `common-events` | `DomainEvent` (base) + 16 event cụ thể (`UserRegisteredEvent`, `ProductCreatedEvent`, `BidPlacedEvent`...) | Định nghĩa mọi sự kiện nghiệp vụ được publish qua Kafka. |

---

## 2. `discovery-server` — danh bạ service (Eureka)

- 1 class duy nhất: `@EnableEurekaServer`, port `8761`.
- Mọi service khác tự đăng ký vào đây khi khởi động + gửi heartbeat định kỳ.
- `api-gateway` hỏi Eureka để biết địa chỉ thật của `user-service`/`catalog-service`/`auction-service`
  thay vì hardcode `localhost:port`.

## 3. `api-gateway` — cổng vào duy nhất (port 8080)

- **Routing** (`application.yml`): `/api/v1/users/**` → `user-service`, `/products/**`+`/categories/**` →
  `catalog-service`, `/auctions/**` → `auction-service`. Dùng `lb://` (load-balanced qua Eureka).
- **`JwtValidationGlobalFilter`**: chạy trước mọi request. Danh sách public (không cần JWT):
  `POST /users/register`, `POST /auth/login`, `/actuator/**`, và **mọi GET** tới products/categories/auctions.
  Còn lại bắt buộc JWT hợp lệ, nếu không → chặn ngay tại gateway, trả 401, **không** tới được service phía sau.
- Gateway chỉ check JWT **hợp lệ hay không** (chữ ký, hạn), không check quyền cụ thể — việc đó đẩy xuống
  tận service xử lý (vì `@RequiresPrivilege`/AOP không chạy được ở gateway).

## 4. `user-service` (port 8081) — trace đầy đủ 1 request `POST /register`

```
UserController.register()
  → @Valid validate RegisterUserRequest (email, password, fullName)
  → UserApiMapper.toCommand()  — đổi "password" thành "rawPassword", record khác, giá trị giống
  → RegisterUserUseCase.register(command)   [@Transactional]
      1. PasswordPolicy.validate()            — rule thuần Java, ≥ 8 ký tự
      2. check email trùng qua UserRepositoryPort
      3. lấy Role mặc định "BUYER" (seed sẵn ở migration V2)
      4. hash password qua PasswordHasherPort
      5. User.register(...) — factory tạo User mới (constructor private)
      6. userRepositoryPort.save(user)        — UserRepositoryAdapter convert User → UserJpaEntity
      7. eventPublisherPort.publish(UserRegisteredEvent)  — ghi vào bảng outbox, CÙNG transaction với (6)
  → trả 201 NGAY — Kafka CHƯA hề được đụng tới lúc này
```

- **`UseCaseConfig`**: nơi duy nhất tiêm 4 Port (interface) vào `RegisterUserUseCase` (class Java thuần,
  không `@Service`) — cầu nối giữa tầng nghiệp vụ "sạch" và tầng hạ tầng (Spring).
- **Không phải request nào cũng qua Mapper**: `login()` và `changePassword()` tự dựng Command bằng tay
  (`new LoginCommand(...)`) vì field đơn giản, không cần đổi tên — Mapper chỉ hữu ích khi có rename
  field hoặc nhiều field.
- **`SecurityConfig`**: lớp phòng thủ thứ 2 (sau gateway) — tự check JWT lại lần nữa ở `user-service`,
  phòng trường hợp ai đó gọi thẳng `localhost:8081` bỏ qua gateway (như demo Kafka tối trước).

## 5. `catalog-service` (port 8082) — Category / Product / Sku

- **Quan hệ**: `Category` tự tham chiếu chính nó qua `parentId` (cây danh mục). 1 `Product` có nhiều
  `Sku` (biến thể: size/màu), **giá nằm ở `Sku`**, không nằm ở `Product`.
- **`CategoryDepthPolicy`**: cây danh mục tối đa 3 cấp (`MAX_DEPTH = 3`) — khớp đúng DoD issue F04.1.
- **Phân quyền 2 lớp** (`ProductOwnershipPolicy`): `@RequiresPrivilege("PRODUCT.UPDATE")` chỉ chứng minh
  "được sửa sản phẩm nói chung", KHÔNG biết sản phẩm cụ thể nào. `ProductOwnershipPolicy` (trong use case)
  mới check `product.sellerId == callerId` — thiếu lớp này thì Seller A sửa được sản phẩm Seller B.
  ADMIN có quyền `PRODUCT.MANAGE_ANY` thì bỏ qua check này.
- **`sellerId` luôn lấy từ JWT**, không bao giờ từ request body — `CreateProductRequest` cố tình không
  có field `sellerId`, tránh 1 client tự khai `sellerId` giả để giả mạo người bán khác.
- **`SearchProductsUseCase`**: chuẩn hóa `categoryId` thành UUID chuẩn TRƯỚC khi đưa vào SQL
  (`CAST(... AS uuid)`) — tránh Postgres tự ném lỗi 500 khi gặp UUID sai định dạng; validate trước để
  trả 400 đúng bản chất hơn. Có N+1 query khi enrich ảnh+SKU (chấp nhận được ở quy mô đồ án).

## 6. `auction-service` (port 8083) — phần phức tạp nhất

### Domain
- `Auction`: giữ `currentHighestBid`/`currentHighestBidderId` ngay trong chính nó. 4 trạng thái:
  `PENDING → ACTIVE → ENDED` (hoặc `CANCELLED`). Nhiều method `withXxx()` riêng cho từng loại thay đổi.
- `Bid`: lịch sử từng lượt đặt giá, không sửa/xóa.

### Race condition + Pessimistic Locking
- **Vấn đề**: 2 người đặt giá cùng lúc, nếu không khóa, cả 2 cùng đọc dữ liệu cũ → cả 2 bid cùng được
  chấp nhận sai (lost update).
- **Giải pháp**: `findByIdForUpdate()` (`SELECT ... FOR UPDATE`) — luồng đầu tiên đọc sẽ khóa dòng DB lại,
  luồng sau phải **đợi** tới khi luồng đầu commit mới được đọc (thấy dữ liệu mới nhất).
- `@Transactional` là thứ giữ khóa tồn tại suốt method — thiếu nó, khóa nhả ngay sau câu `SELECT`,
  mất tác dụng.
- `findById` (không khóa) dùng cho đọc thuần (GET); `findByIdForUpdate` (có khóa) dùng cho mọi use case
  sẽ ghi đè (đặt giá, hủy, start/end).

### `BidValidationPolicy` — 4 điều kiện 1 bid hợp lệ
1. Auction phải `ACTIVE`.
2. `now` phải trước `endTime` — đóng khoảng hở do `AuctionLifecycleJob` có thể trễ tới 10s.
3. Bidder ≠ Seller (không tự đấu giá sản phẩm mình).
4. `amount` ≥ (giá cao nhất hiện tại + bidIncrement), hoặc ≥ startingPrice nếu chưa ai đặt.

### `AntiSnipingPolicy` — chống canh giờ chót
- Nếu có bid trong **5 phút cuối** trước `endTime`, tự động gia hạn `endTime` thêm **5 phút** (tính từ
  `endTime` cũ, không phải từ lúc đặt giá).
- Giới hạn tối đa **12 lần gia hạn** (chống kéo dài vô hạn).

### 2 Job chạy nền
- **`AuctionLifecycleJob`** (mỗi 10s): `PENDING→ACTIVE` khi tới giờ bắt đầu; `ACTIVE→ENDED` khi hết giờ.
  `StartAuctionUseCase`/`EndAuctionUseCase` đều khóa + **check lại lần nữa** dưới khóa (idempotency),
  vì dữ liệu có thể đã đổi giữa lúc job liệt kê danh sách và lúc xử lý từng dòng.
- **`PaymentDeadlineJob`** (mỗi 30s): tìm auction đã `ENDED`, có người thắng, quá hạn thanh toán 24h
  mà chưa trả tiền → phát sự kiện timeout (chưa có consumer xử lý tiếp).

## 7. `infra` — ráp tất cả chạy chung

- **`build-all.sh`**: `mvn install` cho `common-libs` TRƯỚC (đẩy vào cache `~/.m2`), rồi `mvn package`
  từng service — vì Dockerfile chỉ copy sẵn `.jar`, không build Maven trong container.
- **`docker-compose.yml`**: 9 container — 3 Postgres riêng biệt (user/catalog/auction, mỗi service tự
  quản DB mình, không query chéo), 1 Kafka, `discovery-server`, `api-gateway`, 3 service.
- **`depends_on`**: `service_healthy` (đợi Postgres thật sự sẵn sàng qua `pg_isready`) nghiêm ngặt hơn
  `service_started` (chỉ cần container đã bật).
- **Biến môi trường ghi đè `application.yml`**: VD `SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka:9092` — bên
  trong mạng Docker, service gọi nhau bằng **tên container**, không phải `localhost`.

## 8. Các repo "gọi nhau" bằng gì? (polyrepo + Maven)

- **Git không hề kết nối các repo với nhau** — việc `user-service` "có" code `common-libs` xảy ra ở
  tầng Maven, qua khai báo dependency + số version trong `pom.xml`:
  ```xml
  <common-libs.version>1.0.0</common-libs.version>
  <dependency><groupId>com.nexus</groupId><artifactId>common-core</artifactId>
              <version>${common-libs.version}</version></dependency>
  ```
- Maven lấy `.jar` từ 2 nơi (theo thứ tự):
  1. **Cache local `~/.m2`** — có sau khi chạy `mvn install` trên `common-libs` (chính là bước đầu
     `build-all.sh` làm).
  2. **GitHub Packages** — `common-libs` có CI (`.github/workflows/publish.yml`) tự `mvn deploy` mỗi
     lần push lên `main`, đẩy `.jar` lên kho gói riêng của GitHub. Máy khác tải về từ đây nếu chưa có cache.
     (Cần xác thực bằng token cá nhân dù repo là public — giới hạn của GitHub, không phải của project.)
- **Version KHÔNG tự trôi theo**: `user-service` đang pin `1.0.0` dù `common-libs` thực tế đã ở `1.1.0`
  — phải có người chủ động sửa số version + build lại thì thay đổi mới ở `common-libs` mới "tới" được
  `user-service`.

## 9. Kafka + Outbox pattern (xem thêm artifact sơ đồ đã vẽ)

- **Outbox table**: bảng phụ cùng DB với bảng nghiệp vụ. Ghi dữ liệu nghiệp vụ + ghi "sắp có sự kiện gì"
  trong **cùng 1 transaction** — giải quyết "dual write problem" (ghi DB xong nhưng quên gửi Kafka nếu
  crash giữa chừng).
- **`OutboxRelayJob`** (mỗi 5s): quét outbox, publish lên Kafka (`kafkaTemplate.send(topic, payload)`),
  đánh dấu `published_at`.
- Request trả response cho client **trước**, hoàn toàn không chờ Kafka — Kafka chỉ chạy ở bước sau,
  độc lập, nền.
- Hiện project **chỉ có chiều Producer** — chưa có `@KafkaListener`/consumer nào. Để Kafka "thật sự hữu
  dụng" cần 1 service như Notification Service subscribe + consume topic.
- Demo thật đã làm: `POST /register` → thấy `UserRegisteredEvent` xuất hiện trên topic `user-events`
  qua `kafka-console-consumer.sh`.

---

*Ghi chú: file này là tài liệu học tập cá nhân, không thuộc git repo nào trong 7 repo trên.*
