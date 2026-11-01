# ADR-0002: SKU do kho cấp, tồn kho và trạng thái theo SKU + kho, giá theo ngày

**Trạng thái:** Đề xuất (chưa làm) — 2026-11-01. Dự kiến làm ở Sprint 3.

## Bối cảnh

Thầy góp ý bốn điểm về catalog:

1. Mã SKU phải do **kho (fulfillment)** cấp, không phải do catalog tự sinh.
2. Trạng thái bán được phải nằm ở **SKU + kho** (kho này hết thì kho khác còn), không nằm ở sản phẩm.
3. Giá phải quản lý **theo SKU và theo ngày hiệu lực**.
4. Trạng thái nên là enum / có CHECK, không phải chuỗi tự do. (Đã làm xong ở cả 5 service ngày 30/10 và 31/10.)

Hiện trạng (đã kiểm tra trong code):

- catalog-service: bảng `skus(id, product_id, sku_code UNIQUE, price, attributes)`. Mã sinh bởi `CreateProductUseCase`
  (`"SKU-" + 8 ký tự UUID`). Mỗi sản phẩm đúng 1 SKU. `products.status` là DRAFT/ACTIVE/INACTIVE/SOLD.
- fulfillment-service: `inventory_records(sku_id, warehouse_id, total, reserved, unavailable)` đã có `warehouse_id`
  (mặc định một kho), nhưng cột `sku_id` thực tế đang chứa **id sản phẩm**, không phải id SKU
  (xem chú thích trong `V3__create_product_stock_owners.sql`).
- Giá nằm thẳng trong `skus.price`, sửa là mất giá cũ.

## Quyết định đề xuất

### 1. Ai cấp mã SKU

fulfillment-service là nơi **sở hữu SKU**. catalog chỉ giữ tham chiếu.

Luồng:

1. Seller tạo sản phẩm ở catalog (`ProductCreated`, chưa có SKU).
2. Seller (hoặc admin kho) tạo SKU cho sản phẩm: gọi fulfillment, fulfillment sinh `sku_code`
   và lưu vào bảng `skus` của chính nó, rồi phát `SkuCreated(skuCode, productId, attributes)`.
3. catalog nghe `SkuCreated` và giữ bản sao (`sku_snapshots`) để hiển thị, giống cách auction giữ `product_snapshots`.

Lý do: stock gắn với SKU, nên nơi biết kho phải là nơi cấp mã. Không còn hai service cùng "sở hữu" mã.

### 2. Tồn kho và trạng thái theo SKU + kho

Dùng lại `inventory_records` (đã có khoá duy nhất `(sku_id, warehouse_id)`), nhưng:

- `sku_id` trỏ đến **SKU thật** (không còn là id sản phẩm).
- Thêm cột `status` (CHECK): `AVAILABLE`, `OUT_OF_STOCK`, `DISCONTINUED`.
- `AVAILABLE` suy ra từ số lượng: `total - reserved - unavailable > 0`; trạng thái lưu để lọc nhanh.
- Một sản phẩm "còn bán" khi **có ít nhất một kho** ở trạng thái AVAILABLE cho một SKU của nó.
  `products.status` chỉ còn nghĩa nghiệp vụ của seller (DRAFT/ACTIVE/INACTIVE), bỏ SOLD.

Đặt giữ hàng (`ReserveInventoryUseCase`) chọn kho: lấy kho còn hàng, ưu tiên kho gần (xem mục 4).

### 3. Giá theo ngày hiệu lực

Bảng mới `sku_prices` trong catalog-service (giá là dữ liệu bán hàng, nên ở catalog):

| Cột | Ghi chú |
|---|---|
| `id` | UUID |
| `sku_code` | tham chiếu theo mã, không FK xuyên service |
| `price` | `NUMERIC(12,2)`, CHECK `> 0` |
| `valid_from` | `DATE`, bắt buộc |
| `valid_to` | `DATE`, null = đến khi có giá mới |

Luật:

- Giá hiện hành tại ngày D = dòng có `valid_from <= D` và (`valid_to` null hoặc `>= D`), chọn `valid_from` lớn nhất.
- Không cho hai dòng của cùng SKU chồng khoảng ngày (ràng buộc loại trừ `EXCLUDE USING gist` trên khoảng ngày, hoặc kiểm trong use case + test).
- Đổi giá = thêm dòng mới và đóng dòng cũ, **không sửa dòng cũ**. Đơn đã đặt vẫn tra lại được giá lúc đó.
- Bỏ cột `skus.price` sau khi chuyển dữ liệu (migration: mỗi SKU hiện có thành 1 dòng `valid_from = ngày tạo`).

### 4. Giảm giá theo khoảng cách kho

**Không** đưa vào `sku_prices`. Giá niêm yết là một con số theo ngày. Khoảng cách phụ thuộc vào địa chỉ người mua
nên là **luật ở bước checkout** (commerce-service): sau khi biết kho giao và địa chỉ, tính phí/giảm giá rồi ghi
vào dòng đơn hàng như một khoản điều chỉnh riêng. Nhờ vậy giá niêm yết vẫn đơn giản và kiểm tra được.

## Các bước thực hiện (gợi ý cho Sprint 3)

1. fulfillment: bảng `skus`, endpoint tạo SKU, sự kiện `SkuCreated`. (1 ngày)
2. catalog: nghe `SkuCreated`, `sku_snapshots`; bỏ sinh mã trong `CreateProductUseCase`. (0,5 ngày)
3. Chuyển dữ liệu: `inventory_records.sku_id` từ id sản phẩm sang id SKU; commerce dùng SKU khi giữ hàng. (1 ngày, rủi ro cao nhất)
4. catalog: `sku_prices` + use case đổi giá + migration dữ liệu cũ + test chồng ngày. (1 ngày)
5. `inventory_records.status` + chọn kho khi giữ hàng. (0,5 ngày)
6. Luật giảm giá theo khoảng cách ở commerce. (0,5 ngày, làm sau cùng, có thể tách)

## Đánh đổi và rủi ro

- Bước 3 đụng ba service (fulfillment, commerce, catalog) và dữ liệu đang chạy: cần migration có kiểm tra và chạy thử trên cụm Docker.
- Thêm một sự kiện nữa nghĩa là thêm một chỗ có thể lệch dữ liệu (catalog thấy sản phẩm nhưng chưa thấy SKU). UI phải chịu được trạng thái "chưa có SKU".
- Bỏ `products.status = SOLD` cần chỉnh auction-service (đang giữ `product_snapshots.status` với CHECK gồm SOLD, V9) và catalog (V6).

## Câu hỏi còn mở

- Một sản phẩm có nhiều SKU thì SKU khác nhau ở thuộc tính nào (màu, dung lượng)? Hiện `attributes` là chuỗi tự do.
- Đấu giá gắn với sản phẩm hay với SKU? (Hiện auction gắn `product_id`.)
