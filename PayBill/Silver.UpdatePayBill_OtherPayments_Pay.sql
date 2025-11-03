USE [Reporting];
GO

SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROC [Silver].[UpdatePayBill_OtherPayments_Pay]
--(@EarliestDate DATE)
AS
BEGIN
/********************************************************************************** 
EXEC [Silver].[UpdatePayBill_OtherPayments_Pay]
--TRUNCATE TABLE [Silver].[PayBill_OtherPayments_Pay]
SELECT top 10000 * FROM [Silver].[PayBill_OtherPayments_Pay]

/*
====DELETE THIS AFTER USE======
EXEC [Temp].[RobertV_UpdatePerformanceCheck]
    @SprocName = 'Silver.UpdatePayBill_OtherPayments_Pay',
    @DoTruncate = 1,
    @DateParam = '@LoadDate';
*/

Change Log:
Date        Author        Performance (rows/time)																	Ticket - notes
----------  ---------     ------------------------------------------------------									---------------------------
10/16/2025	Robert V	 Initial: Rows 8,936,115 | Time: 1m 24s   ||   Incremental: Rows Added 0 | Time: 0m 28s		TicketNum - Sproc Creation
10/31/2025	Robert V	 Initial: Rows 8,934,744 | Time: 1m 34s   ||   Incremental: Rows Added 0 | Time: 1m 24s		Updated Architecture

***********************************************************************************/
	--DECLARE @MergeResults TABLE
	--(
	--	MergeAction NVARCHAR(10),
	--	InsertedID BIGINT NULL,
	--	DeletedID BIGINT NULL
	--);

	DECLARE @from date = NULL --DATEADD(MONTH,-6, CAST(GETDATE() AS date));
	DECLARE @to   date = CAST(GETDATE() AS date);
	
	IF OBJECT_ID('tempdb..#op') IS NOT NULL DROP TABLE #op;
	SELECT
	    op.Id                    AS OtherPaymentID,
	    op.TimeCardID,
	    op.PaymentTypeId,
	    op.UnitID,
	    op.IsExcludedFromBilling AS BillHold,
	    op.AdjustedInTimecardID  AS AdjustedTimeCardID,
	    CASE WHEN ui.CompanyID = 3 THEN 2 ELSE 1 END AS PayBillRecordTypeID,
	    CASE WHEN CASE WHEN ui.CompanyID = 3 THEN 2 ELSE 1 END = 2 THEN 1 ELSE 0 END AS IsContraRevenue,
	    op.IsEarlyPay,
	    TRY_CAST(op.AppliedForDate AS date) AS ShiftDate,
	    TRY_CAST(op.CreatedDate    AS date) AS BronzeCreatedDate,
	    op.ActualAmount            AS Amount,
	    op.ProcessedDate,
	    CAST(op.ProcessedDate AS date) AS ProcessedDateDate,
	    op.ProcessedBy             AS ProcessedByUserID,
	    CONVERT(binary(32), HASHBYTES('SHA2_256',
	        LOWER(LTRIM(RTRIM(ISNULL(op.Comments,''))))) ) AS NormalizedCommentHash,
	    op.IsDeleted,
		op.IsApproved,
	    CASE WHEN op.IsDeleted = 0 AND op.IsExcludedFromBilling = 1 THEN 0 ELSE 1 END AS IsReportable,
	    CASE
	      WHEN op.CreatedBy = 'c7a2cb0f-7738-4122-9008-7ee30802d8f4'
	           AND CAST(op.CreatedDate AS datetime) IN (
	                '2024-01-10T22:25:00',
					'2024-01-29T22:15:00',
	                '2024-02-12T22:29:00',
					'2024-08-30T09:12:00')
	        THEN 1
	      WHEN op.Comments LIKE '%migration%' THEN 1
	      WHEN op.CreatedBy = 'cbe6b5bb-2de5-40d8-9a8d-08dc37c16b22' THEN 1
	      ELSE 0
	    END AS MigrationFlag
	INTO #op
	FROM nurses.Billing.OtherPayments op WITH (NOLOCK)
	LEFT JOIN nurses.TimeCard.TimeCards tc WITH (NOLOCK) ON tc.Id = op.TimeCardId
	LEFT JOIN nurses.dbo.UserInfo ui       WITH (NOLOCK) ON ui.UserID = tc.NurseId
	WHERE op.ProcessedDate >= COALESCE(@from, '1900-01-01')
	  AND op.ProcessedDate <  DATEADD(DAY, 1, COALESCE(@to, '2079-06-06'))
	  
	CREATE CLUSTERED INDEX CX_op_ProcDate_Id ON #op(ProcessedDateDate, OtherPaymentID);
	CREATE INDEX IX_op_TimeCard              ON #op(TimeCardID);
	CREATE INDEX IX_op_PaymentType           ON #op(PaymentTypeId);
	
	/* Latest status per relevant TimeCard */
	IF OBJECT_ID('tempdb..#tcd_latest') IS NOT NULL DROP TABLE #tcd_latest;
	WITH t AS (
	  SELECT tcd.TimeCardId, tcd.StatusID, tcd.CreatedDate,
	         ROW_NUMBER() OVER (PARTITION BY tcd.TimeCardId ORDER BY tcd.CreatedDate DESC) rn
	  FROM nurses.TimeCard.TimeCardDetails tcd WITH (NOLOCK)
	  INNER JOIN (SELECT DISTINCT TimeCardID FROM #op) k ON k.TimeCardID = tcd.TimeCardId
	)
	SELECT TimeCardId, StatusID
	INTO #tcd_latest
	FROM t WHERE rn = 1;
	
	CREATE UNIQUE CLUSTERED INDEX CX_tcd_latest ON #tcd_latest(TimeCardId);
	
	/* Hash map to comments once */
	IF OBJECT_ID('tempdb..#op_hashes') IS NOT NULL DROP TABLE #op_hashes;
	SELECT DISTINCT NormalizedCommentHash INTO #op_hashes FROM #op;
	CREATE UNIQUE CLUSTERED INDEX CX_op_hashes ON #op_hashes(NormalizedCommentHash);
	
	IF OBJECT_ID('tempdb..#cmap') IS NOT NULL DROP TABLE #cmap;
	SELECT h.NormalizedCommentHash, c.PayBillCommentID
	INTO #cmap
	FROM #op_hashes h
	LEFT JOIN reporting.Silver.PayBill_Comments c WITH (NOLOCK)
	  ON c.Hashed_Comments = h.NormalizedCommentHash;
	
	CREATE UNIQUE CLUSTERED INDEX CX_cmap ON #cmap(NormalizedCommentHash);
	
	/* Merge */
    MERGE reporting.Silver.PayBill_OtherPayments_Pay AS TARGET
    USING
    (
		SELECT
		    op.OtherPaymentID,
		    op.TimeCardID,
		    lit.PayBillLineTypeID,
		    op.UnitID,
		    op.BillHold,
		    op.AdjustedTimeCardID,
		    op.PayBillRecordTypeID,
		    op.IsContraRevenue,
		    op.IsEarlyPay,
		    op.ShiftDate,
		    op.BronzeCreatedDate,
		    NULL        AS ShiftDateStartTime,   -- not applicable for OP
		    NULL        AS ShiftDateEndTime,     -- not applicable for OP
		    NULL        AS Quantity,             -- OP has Amount only
		    NULL        AS Lunch,
		    NULL        AS Rate,
		    op.Amount,
		    ps.PayBillStatusID,
		    DATEADD(HOUR, d.PSTOffset, op.ProcessedDate) AS ProcessedDate,
		    op.ProcessedByUserID,
		    cm.PayBillCommentID,
		    op.IsDeleted,
		    op.IsReportable,
		    0 AS TestFlag,
		    op.MigrationFlag
		FROM #op op 
			LEFT JOIN reporting.PowerBI.Dates d WITH (NOLOCK) ON d.[Date] = op.ProcessedDateDate 
			LEFT JOIN #tcd_latest tcd ON tcd.TimeCardId = op.TimeCardId 
			LEFT JOIN Silver.PayBill_LineItemTypes lit WITH (NOLOCK) ON lit.PaymentTypeId = op.PaymentTypeId 
			LEFT JOIN Silver.PayBill_Statuses ps WITH (NOLOCK) ON ps.PayBillStatusTypeID = 5 AND ps.PayBillSourceStatusID = CONVERT(int,op.IsApproved)
			LEFT JOIN #cmap cm ON cm.NormalizedCommentHash = op.NormalizedCommentHash
    ) AS source
        ON target.[OtherPaymentID] = source.[OtherPaymentID]
    WHEN MATCHED AND 
    (
        ISNULL(target.[TimeCardID], -1)										<> ISNULL(source.[TimeCardID], -1)
        OR ISNULL(target.[PayBillLineTypeID], 0)							<> ISNULL(source.[PayBillLineTypeID], 0)
        OR ISNULL(target.[UnitID], -1)										<> ISNULL(source.[UnitID], -1)
        OR ISNULL(target.[BillHold], -1)									<> ISNULL(source.[BillHold], -1)
        OR ISNULL(target.[AdjustedTimeCardID], -1)							<> ISNULL(source.[AdjustedTimeCardID], -1)
        OR ISNULL(target.[PayBillRecordTypeID], 0)							<> ISNULL(source.[PayBillRecordTypeID], 0)
        OR ISNULL(target.[IsContraRevenue], -1)								<> ISNULL(source.[IsContraRevenue], -1)
        OR ISNULL(target.[IsEarlyPay], -1)									<> ISNULL(source.[IsEarlyPay], -1)
        OR ISNULL(target.[ShiftDate], '')									<> ISNULL(source.[ShiftDate], '')
        OR ISNULL(target.[BronzeCreatedDate], '1900-01-01')					<> ISNULL(source.[BronzeCreatedDate], '1900-01-01')
        OR ISNULL(target.[Amount], -1)										<> ISNULL(source.[Amount], -1)
        OR ISNULL(target.[MigrationFlag], -1)								<> ISNULL(source.[MigrationFlag], -1)
        OR ISNULL(target.[PayBillStatusID], -1)								<> ISNULL(source.[PayBillStatusID], -1)
        OR ISNULL(target.[ProcessedDate], CAST('1900-01-01' AS DATETIME))	<> ISNULL(source.[ProcessedDate], CAST('1900-01-01' AS DATETIME))
        OR ISNULL(target.[ProcessedByUserID], -1)							<> ISNULL(source.[ProcessedByUserID], -1)
        OR ISNULL(target.[PayBillCommentID], -1)							<> ISNULL(source.[PayBillCommentID], -1)
        OR ISNULL(target.[IsDeleted], -1)									<> ISNULL(source.[IsDeleted], -1)
        OR ISNULL(target.[IsReportable], -1)								<> ISNULL(source.[IsReportable], -1)
        --OR ISNULL(target.[TestFlag], -1)             <> ISNULL(source.[TestFlag], -1)
    )
    THEN UPDATE SET
        [TimeCardID]          = source.[TimeCardID]
        ,[PayBillLineTypeID]   = source.[PayBillLineTypeID]
        ,[UnitID]              = source.[UnitID]
        ,[BillHold]			   = source.[BillHold]
        ,[AdjustedTimeCardID]  = source.[AdjustedTimeCardID]
        ,[PayBillRecordTypeID] = source.[PayBillRecordTypeID]
        ,[IsContraRevenue]	   = source.[IsContraRevenue]
        ,[IsEarlyPay]          = source.[IsEarlyPay]
		,[ShiftDate]		   = source.[ShiftDate]
		,[BronzeCreatedDate]   = source.[BronzeCreatedDate]
        ,[Amount]              = source.[Amount]
        ,[MigrationFlag]       = source.[MigrationFlag]
        ,[PayBillStatusID]     = source.[PayBillStatusID]
        ,[ProcessedDate]       = source.[ProcessedDate]
        ,[ProcessedByUserID]   = source.[ProcessedByUserID]
        ,[PayBillCommentID]    = source.[PayBillCommentID]
        ,[IsDeleted]           = source.[IsDeleted]
        ,[IsReportable]        = source.[IsReportable]
        ,[TestFlag]            = source.[TestFlag]
        ,[ETL_ModifiedDate]    = GETDATE()

    WHEN NOT MATCHED BY TARGET THEN
        INSERT
        (
             [OtherPaymentID]
            ,[TimeCardID]
            ,[PayBillLineTypeID]
            ,[UnitID]
            ,[BillHold]
            ,[AdjustedTimeCardID]
            ,[PayBillRecordTypeID]
			,[IsContraRevenue]
            ,[IsEarlyPay]
			,[ShiftDate]
			,[BronzeCreatedDate]
            ,[Amount]
			,[MigrationFlag]
            ,[PayBillStatusID]
            ,[ProcessedDate]
            ,[ProcessedByUserID]
            ,[PayBillCommentID]
            ,[IsDeleted]
            ,[IsReportable]
            ,[TestFlag]
        )
        VALUES
        (
             source.[OtherPaymentID]
            ,source.[TimeCardID]
            ,source.[PayBillLineTypeID]
            ,source.[UnitID]
            ,source.[BillHold]
            ,source.[AdjustedTimeCardID]
            ,source.[PayBillRecordTypeID]
			,source.[IsContraRevenue]
            ,source.[IsEarlyPay]
			,source.[ShiftDate]
			,source.[BronzeCreatedDate]
            ,source.[Amount]
			,source.[MigrationFlag]
            ,source.[PayBillStatusID]
            ,source.[ProcessedDate]
            ,source.[ProcessedByUserID]
            ,source.[PayBillCommentID]
            ,source.[IsDeleted]
            ,source.[IsReportable]
            ,source.[TestFlag]
        )

    WHEN NOT MATCHED BY SOURCE THEN 
        DELETE;
	--OUTPUT
	--    $action AS MergeAction,
	--    inserted.[OtherPaymentID],
	--    deleted.[OtherPaymentID]
	--INTO @MergeResults;
	
	---- Aggregate:
	--SELECT
	--    MergeAction,
	--    COUNT(InsertedID) AS InsertCount,
	--    COUNT(DeletedID) AS DeleteCount
	--FROM @MergeResults
	--GROUP BY MergeAction;
-----------------------------------------------------------------------------------------------------------
		 --$action AS [MergeAction],
		 --inserted.[OtherPaymentID],
		 --deleted.[StatusID] 'deleted',
		 --inserted.[StatusID] 'inserted',
		 --CASE WHEN ISNULL(deleted.[TimeCardID], -1) <> ISNULL(inserted.[TimeCardID], -1) THEN 'TimeCardID ' ELSE '' END +
		 --CASE WHEN ISNULL(deleted.[PayBillLineTypeID], -1) <> ISNULL(inserted.[PayBillLineTypeID], -1) THEN 'PayBillLineTypeID ' ELSE '' END +
		 --CASE WHEN ISNULL(deleted.[UnitID], -1) <> ISNULL(inserted.[UnitID], -1) THEN 'UnitID ' ELSE '' END +
		 ----CASE WHEN ISNULL(deleted.[BillHold], -1) <> ISNULL(inserted.[BillHold], -1) THEN 'BillHold ' ELSE '' END +
		 --CASE WHEN ISNULL(deleted.[AdjustedTimeCardID], -1) <> ISNULL(inserted.[AdjustedTimeCardID], -1) THEN 'AdjustedTimeCardID ' ELSE '' END +
		 --CASE WHEN ISNULL(deleted.[PayBillRecordTypeID], -1) <> ISNULL(inserted.[PayBillRecordTypeID], -1) THEN 'PayBillRecordTypeID ' ELSE '' END +
		 --CASE WHEN ISNULL(deleted.[IsEarlyPay], -1) <> ISNULL(inserted.[IsEarlyPay], -1) THEN 'IsEarlyPay ' ELSE '' END +
		 --CASE WHEN ISNULL(deleted.[ShiftDate], '1900-01-01') <> ISNULL(inserted.[ShiftDate], '1900-01-01') THEN 'ShiftDate ' ELSE '' END +
		 --CASE WHEN ISNULL(deleted.[Amount], -1) <> ISNULL(inserted.[Amount], -1) THEN 'Amount ' ELSE '' END +
		 --CASE WHEN ISNULL(deleted.[MigrationFlag], -1) <> ISNULL(inserted.[MigrationFlag], -1) THEN 'MigrationFlag ' ELSE '' END +
		 --CASE WHEN ISNULL(deleted.[StatusID], -1) <> ISNULL(inserted.[StatusID], -1) THEN 'StatusID ' ELSE '' END +
		 --CASE WHEN ISNULL(deleted.[ProcessedDate], '1900-01-01') <> ISNULL(inserted.[ProcessedDate], '1900-01-01') THEN 'ProcessedDate ' ELSE '' END +
		 --CASE WHEN ISNULL(deleted.[ProcessedByUserID], -1) <> ISNULL(inserted.[ProcessedByUserID], -1) THEN 'ProcessedByUserID ' ELSE '' END +
		 --CASE WHEN ISNULL(deleted.[PayBillCommentID], -1) <> ISNULL(inserted.[PayBillCommentID], -1) THEN 'PayBillCommentID ' ELSE '' END +
		 --CASE WHEN ISNULL(deleted.[IsDeleted], -1) <> ISNULL(inserted.[IsDeleted], -1) THEN 'IsDeleted ' ELSE '' END +
		 --CASE WHEN ISNULL(deleted.[IsReportable], -1) <> ISNULL(inserted.[IsReportable], -1) THEN 'IsReportable ' ELSE '' END 
		 --AS [ChangedColumns];

END;
 