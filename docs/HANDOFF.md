# Project Nexus — Handoff / Trạng thái hiện tại

**Cập nhật lần cuối:** 2026-09-25, trước khi chuyển máy làm việc từ Windows sang MacBook.
File này tồn tại vì bộ nhớ hội thoại (memory) của Claude Code gắn theo từng máy — đổi máy
là mất ngữ cảnh, nên toàn bộ trạng thái quan trọng được chép lại đây, trong git, để một
conversation mới (trên máy khác) đọc file này là nắm được đang làm tới đâu.

## 1. Bối cảnh chung

Project Nexus là đồ án tốt nghiệp/capstone môn học tại FPT, domain: **e-commerce có chức
năng đấu giá**. SRS gốc: "Project Nexus SRS v1.0" (04/06/2026, tác giả HauNK).

Nhóm 4 người: Trần Nguyễn Minh An (leader, chính là user của conversation này,
trannguyenminhan2005@gmail.com), Trịnh Hoàng Mai Anh, Vũ Thị Tú Anh, Phan Anh Khoa.

**Có 2 codebase tách biệt, đừng nhầm:**
- **Solo repo** (`antran19/project-nexus`, Maven monorepo) — dự án cá nhân của leader, làm
  trước cả nhóm để học kiến trúc. **Hiện đang tạm gác lại**, không phải hướng đi hiện tại.
- **Polyrepo** (6 repo GitHub riêng biệt dưới `antran19`) — **đây là hướng đang làm**, theo
  yêu cầu của thầy: mỗi microservice phải là 1 repo + 1 project Spring Boot độc lập, không
  dùng monorepo chung cho bài nộp của nhóm.

## 2. Polyrepo — 6 repo hiện có

Clone tất cả làm sibling trong cùng 1 thư mục cha (xem `infra/README.md`):

```
<parent>/
  common-libs/
  discovery-server/
  api-gateway/
  user-service/
  catalog-service/
  infra/
```

Tất cả đều public trên GitHub, dưới account `antran19`. Tính đến 2026-09-25, cả 6 repo đều
sạch (`main` == `origin/main`, không có thay đổi chưa commit).

## 3. Đã implement xong (verify trực tiếp từ code, không chỉ suy đoán)

**User Service** — `POST /api/v1/users/register`, `POST /api/v1/auth/login` (JWT),
`PUT /api/v1/users/me/password`. Role/privilege-based authorization.

**Catalog Service** — Category CRUD đầy đủ; Product CRUD + `PATCH /{id}/status` +
`GET /discover` + `GET /{id}` + search (Postgres full-text search); SKU, product image;
domain event publish qua Outbox pattern → Kafka topic `catalog-events`
(`ProductCreated`, `ProductUpdated`, `ProductStatusChanged`, `CategoryCreated`,
`CategoryUpdated`).

**Hạ tầng** — `discovery-server` (Eureka), `api-gateway` (JWT signature/expiry check +
method-aware routing: GET public, còn lại cần auth), `common-libs` (4 module:
`common-core`, `common-web`, `common-events`, `common-security`, publish qua GitHub
Packages, version hiện tại `1.0.0`), `infra` (docker-compose chạy cả cụm, đã verify
end-to-end: Eureka registration, register/login/category/product/search qua gateway).

## 4. Chưa implement gì cả (chưa có repo, chưa có 1 dòng code)

- **Notification Service**
- **Commerce Service**
- **Auction Service** — chức năng lõi "đấu giá" của domain, **đang thiết kế dở** (xem mục 5)
- **Fulfillment Service**

## 5. ĐANG LÀM: Thiết kế Auction Service — CHƯA XONG, đang chờ quyết định

Đang theo quy trình `superpowers:brainstorming` (path **architectural** — vì đây là service
mới hoàn toàn, ảnh hưởng cách các service khác tương tác): tìm hiểu bối cảnh → hỏi làm rõ →
đề xuất phương án → thiết kế theo section → viết spec file → rồi mới chuyển sang
`writing-plans` để lên kế hoạch implement. **Chưa viết code nào cho Auction Service cả.**

### 5.1. Requirement đã trích xuất từ SRS §3.5 (Auction Services)

**Auction Management:**
- Create Auction (seller): cần `productId`, `sellerId`, `startingPrice`, `bidIncrement`,
  `startTime`, `endTime`. Validate seller eligibility, product validity, không có auction
  active nào khác cho cùng product.
- Configure Auction: chỉ sửa được **trước khi** auction bắt đầu.
- Lifecycle tự động: `PENDING → ACTIVE` (tới giờ start) `→ ENDED` (tới giờ end). Không cho
  chuyển trạng thái sai quy tắc.
- Cancel Auction: seller cancel (có điều kiện eligibility) + Admin force cancel
  (`AUCTION.ADMIN_CANCEL`). Cancel xong thì chặn mọi bid tiếp theo. Có audit log.
- Visibility: public/restricted (chưa rõ chi tiết, có thể đơn giản hóa MVP = luôn public).

**Bidding:**
- Place Bid: auction phải đang active, bid amount phải ≥ giá cao nhất hiện tại +
  `bidIncrement`, xử lý atomic.
- Concurrency: nhiều người bid cùng lúc — phải đảm bảo bid cao nhất hợp lệ luôn được ghi
  nhận đúng, không bị mất do race condition. **Chưa chốt cơ chế** (pessimistic lock vs
  optimistic lock) — đây là 1 "approach" cần quyết định ở bước tiếp theo.
- Bid history: lưu bidder, amount, thời điểm.
- Outbid detection: khi có người trả giá cao hơn, phát event `Outbid` cho người vừa bị vượt.
- Anti-sniping: nếu có bid đặt gần sát giờ kết thúc thì tự động gia hạn thêm
  `ANTI_SNIPING_EXTENSION_MINUTES` (mặc định 5 phút). SRS không cho số giới hạn tổng số lần
  gia hạn — **cần tự đề xuất 1 con số hợp lý và note rõ đây là quyết định tự thêm, không có
  trong SRS**.

**Settlement:**
- Khi auction kết thúc: khóa auction, không nhận bid nữa.
- Xác định người thắng = bid hợp lệ cao nhất. Không có bid nào → auction thất bại
  (`AuctionFailed`).
- Phát event `AuctionWon` (thành công) hoặc `AuctionFailed` (không ai bid).
- Gửi settlement info (product, giá cuối, buyer, seller) — **Auction Service KHÔNG tự tạo
  order**, chỉ gửi đi cho Commerce Service xử lý tiếp.
- Payment deadline: người thắng có `AUCTION_PAYMENT_DEADLINE_HOURS` (mặc định 24h) để thanh
  toán. Không thanh toán kịp → phát event `AuctionPaymentTimeout` + phạt điểm uy tín.
- Idempotent bắt buộc cho: place bid, auction ending, settlement.

**Events cần phát (Kafka, theo Outbox pattern giống Catalog/User):** `AuctionCreated`,
`AuctionScheduled`, `AuctionStarted`, `BidPlaced`, `Outbid`, `AuctionCancelled`,
`AuctionEnded`, `AuctionWon`, `AuctionFailed`, `AuctionPaymentTimeout`, `AuctionSettled`.

**Privileges cần thêm vào `common-security`:** `AUCTION.CREATE`, `AUCTION.UPDATE`,
`AUCTION.CANCEL`, `AUCTION.ADMIN_CANCEL`, `AUCTION.VIEW`, `AUCTION.LIST`, `AUCTION.BID`,
`AUCTION.VIEW_BID_HISTORY`.

**Config constants liên quan (từ SRS):**
| Constant | Giá trị mặc định |
|---|---|
| `AUCTION_MIN_DURATION_MINUTES` | 60 |
| `AUCTION_MAX_DURATION_HOURS` | 168 |
| `DEFAULT_BID_INCREMENT` | 10 |
| `MAX_ACTIVE_AUCTIONS_PER_SELLER` | 5 |
| `ANTI_SNIPING_ENABLED` | true |
| `ANTI_SNIPING_EXTENSION_MINUTES` | 5 |
| `AUCTION_PAYMENT_DEADLINE_HOURS` | 24 |
| `MIN_REPUTATION_TO_BID` | (có nhưng chưa có hệ thống reputation nào để check) |
| `MIN_REPUTATION_TO_CREATE_AUCTION` | (tương tự) |

**Yêu cầu phi chức năng đáng chú ý:** bid placement là critical path, tối ưu độ trễ thấp;
throughput mục tiêu ≥ 3,000 bids/phút; service phải stateless (trừ cache/optimization tạm
thời); không được mất bid hợp lệ nào kể cả khi service restart; audit log cho kết quả
auction, không được sửa.

### 5.2. Vấn đề đã phát hiện: 3 điểm SRS yêu cầu nhưng phụ thuộc service chưa tồn tại

1. **Reputation check** (`MIN_REPUTATION_TO_BID`, `MIN_REPUTATION_TO_CREATE_AUCTION`) —
   chưa có hệ thống điểm uy tín nào được xây ở đâu cả (User Service mới chỉ có
   register/login/đổi mật khẩu).
2. **Gửi settlement cho Commerce để tạo order** — Commerce Service chưa tồn tại.
3. **Payment deadline enforcement** — việc xác nhận "đã thanh toán" là của Commerce, Auction
   Service không có cách nào biết được.

### 5.3. Đề xuất đã đưa ra cho user (ĐANG CHỜ USER XÁC NHẬN — chưa được đồng ý)

Build **đầy đủ 100% phần lõi Auction tự làm được** (lifecycle, bidding, concurrency,
anti-sniping, xác định người thắng, tự đóng phiên đúng giờ). Với 3 điểm phụ thuộc ở trên:
Auction Service **chỉ phát Kafka event** (`AuctionWon`, `AuctionPaymentTimeout`...), không tự
xử lý thay các service chưa tồn tại. Đây chính là pattern đã dùng khi build Catalog Service
lúc Commerce chưa có (check "product có order không" tạm thời trả về `true`/no-op, có ghi
chú rõ trong code).

**Trạng thái: user nói "chưa hiểu, giải thích lại" → đã giải thích lại bằng ví dụ đơn giản
hơn (ẩn dụ "hô lên rồi ai nghe thì xử lý"). Sau đó user chuyển sang yêu cầu viết file
handoff này, nên vẫn CHƯA CÓ câu trả lời cuối cùng cho đề xuất ở trên.**

## 6. Bước tiếp theo khi resume (trên MacBook)

1. **Trước tiên, hỏi lại user có đồng ý với đề xuất ở mục 5.3 không** (chưa được confirm).
2. Nếu đồng ý → chuyển sang bước "Propose 2-3 approaches" của brainstorming skill: quyết
   định cơ chế concurrency cho bidding (pessimistic row lock vs optimistic lock + retry —
   gợi ý nghiêng về pessimistic vì đơn giản và an toàn hơn ở mức 1 auction bị nhiều người
   tranh giành cùng lúc) và cơ chế lifecycle scheduling (poll định kỳ kiểu `@Scheduled`,
   giống `OutboxRelayJob` đã dùng ở Catalog/User).
3. Viết design spec đầy đủ theo đúng khuôn mẫu đã dùng cho Catalog Service (xem file tham
   khảo bên dưới), lưu tại `docs/superpowers/specs/YYYY-MM-DD-auction-service-design.md`
   trong 1 repo phù hợp (repo `infra` này, hoặc trong chính repo `auction-service` mới sau
   khi tạo — cần quyết định).
4. Tạo repo GitHub mới `antran19/auction-service`, bootstrap Spring Boot project theo đúng
   khuôn mẫu polyrepo hiện có (Eureka client, phụ thuộc `common-libs`, CI, Dockerfile) —
   giống hệt cách `catalog-service` đã được dựng.
5. Invoke skill `writing-plans` để ra implementation plan, rồi mới code.

**File tham khảo phong cách/chi tiết đã dùng cho Catalog Service** (để giữ nhất quán style
khi viết spec cho Auction Service) — nằm trong solo repo, KHÔNG có trên Mac trừ khi clone
solo repo về:
`project-nexus/docs/superpowers/specs/2026-09-24-catalog-service-design.md` (nhánh
`worktree-platform-foundation-user-service`).

## 7. QUAN TRỌNG — file cần copy thủ công sang Mac (không nằm trong git)

- **SRS gốc:** `D:\Downloads\srs-nexus-ecommerce-auction-v1.docx` — **bắt buộc phải copy**,
  đây là nguồn duy nhất chứa yêu cầu chi tiết cho Notification/Commerce/Fulfillment (mục 5.1
  ở trên mới chỉ trích Auction). Không có file này trên Mac thì không đọc được SRS gốc.
- Excel plan của nhóm: `C:\Users\Lenovo\Downloads\Nexus-Team2-Plan-3Months.xlsx` (và file
  tham khảo gốc `Nexus-Backlog-Plan.xlsx` cùng thư mục) — cần nếu muốn xem/sửa kế hoạch
  sprint/capacity.

## 8. Ghi chú môi trường & cách làm việc

- Trên máy Windows này từng gặp hiện tượng file bị chỉnh sửa/nhân bản ngoài ý muốn (nghi do
  IDE background service) — chưa rõ có xảy ra trên Mac không, nhưng nên cẩn trọng, luôn đọc
  kỹ diff/nội dung trước khi tin `git status`.
- Sở thích làm việc của user: một khi đã đang thực thi/xác nhận thì hỏi ít, hỏi 1 câu rõ
  ràng thay vì hỏi dồn nhiều lựa chọn; nếu yêu cầu mơ hồ về việc đang nhắm vào artifact/thread
  nào thì hỏi thẳng thay vì đoán; giữ doc/spec ngắn gọn, đúng trọng tâm.
- Repo solo (`project-nexus`) có PR #1 đang mở, và 35 commit của Catalog Service đã làm xong
  nhưng chưa push lên remote — việc merge/PR/giữ nguyên **vẫn chưa được quyết định**, không
  liên quan gì đến polyrepo đang làm, có thể bỏ qua cho tới khi quay lại solo repo.
