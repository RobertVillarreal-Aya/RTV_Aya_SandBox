  USE [reporting];
GO

CREATE OR ALTER VIEW [Silver].[PayBill_Details]
AS



------------------------------------------------------------
-- PayrollDetails (PayBillTypeID = 0)
------------------------------------------------------------
SELECT 
    spb.[PayBillID],
	pb.[PayrollDetailsID]						AS [PayrollDetailsID],
    NULL		 								AS [OtherPaymentID],
    CAST(0 AS TINYINT)							AS [PayBillTypeID],
    pb.[TimeCardID],
    pb.[PayBillLineTypeID],
    pb.[UnitID],
    pb.[ExcludeFromPayBill],
	NULL										AS [BillHold],
    pb.[AdjustedTimeCardID],
    pb.[PayBillRecordTypeID],
    pb.[IsContraRevenue],
    pb.[IsEarlyPay],
    pb.[ShiftDate],
	pb.[BronzeCreatedDate],
    pb.[ShiftDateStartTime],
    pb.[ShiftDateEndTime],
	pb.[Lunch],
	pb.[Quantity],
	pb.[Rate],
    pb.[Amount],
    pb.[MigrationFlag],
    pb.[PayBillStatusID],
    pb.[ProcessedDate],
    pb.[ProcessedByUserID],
    pb.[PayBillCommentID],
	rt.[IsPay],
	rt.[IsBill],
    pb.[IsDeleted],
    pb.[IsReportable],
    pb.[TestFlag],
    pb.[ETL_CreatedDate],
    pb.[ETL_ModifiedDate]
FROM [Silver].[PayBill_PayrollDetails] Pb WITH (NOLOCK)
LEFT JOIN [Silver].[PayBill] spb WITH (NOLOCK) ON spb.[PayBillTypeID] = 0 AND spb.[PayrollSourceID] = pb.[PayrollDetailsID]
LEFT JOIN [Silver].[PayBill_RecordTypes] rt WITH (NOLOCK) ON rt.[PayBillRecordTypeID] = pb.[PayBillRecordTypeID] AND rt.PayBillRecordTypeClass = 'TransactionType'

UNION ALL

------------------------------------------------------------
-- OtherPayments_Pay (PayBillTypeID = 1)
------------------------------------------------------------
SELECT 
    spb.[PayBillID],
	NULL										AS [PayrollDetailsID],
    pb.[OtherPaymentID]							AS [OtherPaymentID],
    CAST(1 AS TINYINT)							AS [PayBillTypeID],
    pb.[TimeCardID],
    pb.[PayBillLineTypeID],
    pb.[UnitID],
    NULL										AS [ExcludeFromPayBill],
	pb.[BillHold],
    pb.[AdjustedTimeCardID],
    pb.[PayBillRecordTypeID],
    pb.[IsContraRevenue],
    pb.[IsEarlyPay],
    pb.[ShiftDate],
	pb.[BronzeCreatedDate],
    NULL										AS [ShiftDateStartTime],
    NULL										AS [ShiftDateEndTime],
	NULL										AS [Lunch],
	NULL										AS [Quantity],
	NULL										AS [Rate],
    pb.[Amount],
    pb.[MigrationFlag],
    pb.[PayBillStatusID],
    pb.[ProcessedDate],
    pb.[ProcessedByUserID],
    pb.[PayBillCommentID],
	rt.[IsPay],
	rt.[IsBill],
    pb.[IsDeleted],
    pb.[IsReportable],
    pb.[TestFlag],
    pb.[ETL_CreatedDate],
    pb.[ETL_ModifiedDate]
FROM [Silver].[PayBill_OtherPayments_Pay] pb WITH (NOLOCK)
LEFT JOIN [Silver].[PayBill] spb WITH (NOLOCK) ON spb.[PayBillTypeID] = 1 AND spb.[PayrollSourceID] = pb.[OtherPaymentID]
LEFT JOIN [Silver].[PayBill_RecordTypes] rt WITH (NOLOCK) ON rt.[PayBillRecordTypeID] = pb.[PayBillRecordTypeID] AND rt.PayBillRecordTypeClass = 'TransactionType'

UNION ALL

------------------------------------------------------------
-- OtherPayments_Bill (PayBillTypeID = 2)
------------------------------------------------------------
SELECT 
    spb.[PayBillID],
	NULL										AS [PayrollDetailsID],
    pb.[OtherPaymentID]							AS [OtherPaymentID],
    CAST(2 AS TINYINT)                          AS [PayBillTypeID],
    pb.[TimeCardID],
    pb.[PayBillLineTypeID],
    pb.[UnitID],
    NULL										AS [ExcludeFromPayBill],
	pb.[BillHold],
    pb.[AdjustedTimeCardID],
    pb.[PayBillRecordTypeID],
    pb.[IsContraRevenue],
    pb.[IsEarlyPay],
    pb.[ShiftDate],
	pb.[BronzeCreatedDate],
    NULL										AS [ShiftDateStartTime],
    NULL										AS [ShiftDateEndTime],
	NULL										AS [Lunch],
	NULL										AS [Quantity],
	NULL										AS [Rate],
    pb.[Amount],
    pb.[MigrationFlag],
    pb.[PayBillStatusID],
    pb.[ProcessedDate],
    pb.[ProcessedByUserID],
    pb.[PayBillCommentID],
	rt.[IsPay],
	rt.[IsBill],    
	pb.[IsDeleted],
    pb.[IsReportable],
    pb.[TestFlag],
    pb.[ETL_CreatedDate],
    pb.[ETL_ModifiedDate]
FROM [Silver].[PayBill_OtherPayments_Bill] pb WITH (NOLOCK)
LEFT JOIN [Silver].[PayBill] spb WITH (NOLOCK) ON spb.[PayBillTypeID] = 2 AND spb.[PayrollSourceID] = pb.[OtherPaymentID]
LEFT JOIN [Silver].[PayBill_RecordTypes] rt WITH (NOLOCK) ON rt.[PayBillRecordTypeID] = pb.[PayBillRecordTypeID] AND rt.PayBillRecordTypeClass = 'TransactionType'

;