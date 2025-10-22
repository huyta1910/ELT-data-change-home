WITH CTE AS (
--- REVENUE
SELECT CuaHang.code, CuaHang.name, 
CAST(Ca.date AS DATE) AS date,
HoaDon.id AS bill_id, 
HoaDon.room_id AS room, 
HoaDon.start AS start, 
HoaDon.finish AS finish,
COUNT(HoaDon.id) AS invoices, 
SUM(HoaDon.guests) AS guests, 
ROUND(SUM(HoaDon.sum), 0) AS sum, 
ROUND(SUM(HoaDon.total), 0) AS total, 
ROUND(SUM(HoaDon.vat), 0)  AS vat, 
ROUND(SUM(HoaDon.phuphi), 0) AS phuphi

FROM CuaHang, Ca, HoaDon

WHERE CuaHang.code = Ca.cuahang_id AND 
Ca.id = HoaDon.ca_id AND 
Ca.cuahang_id = HoaDon.cuahang_id AND 
Ca.cuahang_id IN ('1', '4', '5', '6', '7', '9', '10', '11', '12', '13', '14', '16', '17', '18', '19', '22', '23', '24', '25', '29', '31', '32', '33','35') AND 
CAST(Ca.date AS DATE) >= '2018-01-01' AND 
CAST(Ca.date AS DATE) <= '2025-10-20' AND 
CuaHang.trangthai = 4 AND 
Ca.trangthai = 4 AND 
HoaDon.trangthai = 4 

GROUP BY CuaHang.code, CuaHang.name, 
CAST(Ca.date AS DATE), HoaDon.id, HoaDon.room_id, HoaDon.start, HoaDon.finish
), 
CTE2 AS (
-- GOODS_DISCOUNT
SELECT CuaHang.code, 
CAST(Ca.date AS DATE) AS date,
HoaDon.id AS bill_id, 
SUM(HoaDon_GiamGia.total) AS goods_discount

FROM CuaHang, Ca, HoaDon, HoaDon_GiamGia, HoaDon_ChiTietHangHoa

WHERE CuaHang.code = Ca.cuahang_id AND 
Ca.id = HoaDon.ca_id AND 
Ca.cuahang_id = HoaDon.cuahang_id AND 
HoaDon.id = HoaDon_ChiTietHangHoa.bill_id AND 
HoaDon.cuahang_id = HoaDon_ChiTietHangHoa.cuahang_id AND 
HoaDon.id = HoaDon_GiamGia.bill_id AND 
HoaDon.cuahang_id = HoaDon_GiamGia.cuahang_id AND 
Ca.cuahang_id IN ('1', '4', '5', '6', '7', '9', '10', '11', '12', '13', '14', '16', '17', '18', '19', '22', '23', '24', '25', '29', '31', '32', '33','35') AND 
HoaDon_GiamGia.code2 = HoaDon_ChiTietHangHoa.id AND 
CAST(Ca.date AS DATE) >= '2018-01-01' AND 
CAST(Ca.date AS DATE) <= '2025-10-20' AND 
CuaHang.trangthai = 4 AND 
Ca.trangthai = 4 AND 
HoaDon.trangthai = 4 AND 
HoaDon_GiamGia.trangthai = 4 AND 
HoaDon_ChiTietHangHoa.trangthai = 4 

GROUP BY CuaHang.code, CAST(Ca.date AS DATE), HoaDon.id
),
CTE3 AS (
--- CUSTOMER_DISCOUNT
SELECT CuaHang.code, 
CAST(Ca.date AS DATE) AS date, 
HoaDon.id AS bill_id,
SUM(HoaDon_GiamGia.total) AS customer_discount

FROM CuaHang, Ca, HoaDon, HoaDon_GiamGia

WHERE CuaHang.code = Ca.cuahang_id AND 
Ca.id = HoaDon.ca_id AND 
Ca.cuahang_id = HoaDon.cuahang_id AND 
HoaDon.id = HoaDon_GiamGia.bill_id AND 
HoaDon.cuahang_id = HoaDon_GiamGia.cuahang_id AND
HoaDon_GiamGia.code1 = 3 AND  
Ca.cuahang_id IN ('1', '4', '5', '6', '7', '9', '10', '11', '12', '13', '14', '16', '17', '18', '19', '22', '23', '24', '25', '29', '31', '32', '33','35') AND 
CAST(Ca.date AS DATE) >= '2018-01-01' AND 
CAST(Ca.date AS DATE) <= '2025-10-20' AND 
CuaHang.trangthai = 4 AND 
Ca.trangthai = 4 AND 
HoaDon.trangthai = 4 AND 
HoaDon_GiamGia.trangthai = 4

GROUP BY CuaHang.code, CAST(Ca.date AS DATE), HoaDon.id

),
CTE4 AS (
-- INVOICE_DISCOUNT
SELECT CuaHang.code, 
CAST(Ca.date AS DATE) AS date,
HoaDon.id AS bill_id, 
SUM(HoaDon_GiamGia.total) AS invoice_discount

FROM CuaHang, Ca, HoaDon, HoaDon_GiamGia

WHERE CuaHang.code = Ca.cuahang_id AND 
Ca.id = HoaDon.ca_id AND 
Ca.cuahang_id = HoaDon.cuahang_id AND
HoaDon.id = HoaDon_GiamGia.bill_id AND 
HoaDon.cuahang_id = HoaDon_GiamGia.cuahang_id AND 
HoaDon_GiamGia.code1 = 4 AND 
Ca.cuahang_id IN ('1', '4', '5', '6', '7', '9', '10', '11', '12', '13', '14', '16', '17', '18', '19', '22', '23', '24', '25', '29', '31', '32', '33','35') AND 
CAST(Ca.date AS DATE) >= '2018-01-01' AND 
CAST(Ca.date AS DATE) <= '2025-10-20' AND 
CuaHang.trangthai = 4 AND 
Ca.trangthai = 4 AND 
HoaDon.trangthai = 4 AND 
HoaDon_GiamGia.trangthai = 4

GROUP BY CuaHang.code, CAST(Ca.date AS DATE), HoaDon.id
)
SELECT CTE.bill_id, CTE.code, CTE.name AS [Chi nhánh], CTE.room AS [Phòng], CTE.date AS [Ngày], CTE.guests AS [Số khách], CTE.total, 
CTE.vat, CTE.phuphi,
ISNULL(CTE.sum, 0) + ISNULL(CTE.vat, 0) + ISNULL(CTE.phuphi, 0) - ISNULL(CTE2.goods_discount, 0) - ISNULL(CTE3.customer_discount, 0) - ISNULL(CTE4.invoice_discount, 0) AS [Doanh Thu],
ISNULL(CTE.sum, 0) + ISNULL(CTE.vat, 0) + ISNULL(CTE.phuphi, 0) AS [Doanh Số],
CTE.start, CTE.finish
FROM CTE
LEFT JOIN CTE2 ON CTE.date = CTE2.date AND CTE.code = CTE2.code AND CTE.bill_id = CTE2.bill_id
LEFT JOIN CTE3 ON CTE.date = CTE3.date AND CTE.code = CTE3.code AND CTE.bill_id = CTE3.bill_id
LEFT JOIN CTE4 ON CTE.date = CTE4.date AND CTE.code = CTE4.code AND CTE.bill_id = CTE4.bill_id
ORDER BY [Chi nhánh]

