USE [Reporting];
GO

SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROC [Silver].[UpdatePayBill_PayrollDetails]
--(@EarliestDate DATE)(
AS BEGIN
/********************************************************************************** 
EXEC [Silver].[UpdatePayBill_PayrollDetails]
--TRUNCATE TABLE [Silver].[PayBill_PayrollDetails]
SELECT TOP 10000 * FROM [Silver].[PayBill_PayrollDetails]
SELECT COUNT(*) FROM [Silver].[PayBill_PayrollDetails]


/*
====DELETE THIS AFTER USE======
EXEC [Temp].[RobertV_UpdatePerformanceCheck]
    @SprocName = 'Silver.UpdatePayBill_PayrollDetails',
    @DoTruncate = 1,                        
    @DateParam = '@LoadDate';     
*/

Change Log:

Date        Author        Performance (rows/time)																	Ticket - notes
----------  ---------     ------------------------------------------------------									---------------------------
10/16/2025	Robert V	 Initial: Rows 7386533 | Time: 3m 30s   ||   Incremental: Rows Added 2 | Time: 1m 55s		TicketNum - Sproc Creation
10/31/2025	Robert V	 Initial: Rows 44,599,058 | Time: 8m 57s   ||   Incremental: Rows Added 0 | Time: 9m 57s	Updated Architecure

/* ==========================================================

   RowCounts
   Source: nurses.Billing.PayrollDetails
   ========================================================== */
   Note:
   - CreatedDate and UpdatedDate are not indexed, and using them (or GREATEST) forces a full scan.
   - ProcessedDate is the only column with a leading index
		(IX_PayrollDetails_ProcessedDate_PayrollDetailsTypeId_IsDeleted),
		allowing an efficient Index Seek and much faster runtime.
   ==========================================================

SELECT COUNT (*) FROM [nurses].[Billing].[PayrollDetails]

SELECT 
    '1 Year' AS Period,
    FORMAT(COUNT(*), 'N0') AS RowCounter
FROM nurses.Billing.PayrollDetails AS PayDetails
WHERE PayDetails.ProcessedDate >= DATEADD(YEAR, -1, GETDATE())

UNION ALL
SELECT 
    '6 Months' AS Period,
    FORMAT(COUNT(*), 'N0') AS RowCounter
FROM nurses.Billing.PayrollDetails AS PayDetails
WHERE PayDetails.ProcessedDate >= DATEADD(MONTH, -6, GETDATE())

UNION ALL
SELECT 
    '3 Months' AS Period,
    FORMAT(COUNT(*), 'N0') AS RowCounter
FROM nurses.Billing.PayrollDetails AS PayDetails
WHERE PayDetails.ProcessedDate >= DATEADD(MONTH, -3, GETDATE())

UNION ALL
SELECT 
    '1 Month' AS Period,
    FORMAT(COUNT(*), 'N0') AS RowCounter
FROM nurses.Billing.PayrollDetails AS PayDetails
WHERE PayDetails.ProcessedDate >= DATEADD(MONTH, -1, GETDATE())

UNION ALL
SELECT 
    '1 Week' AS Period,
    FORMAT(COUNT(*), 'N0') AS RowCounter
FROM nurses.Billing.PayrollDetails AS PayDetails
WHERE PayDetails.ProcessedDate >= DATEADD(WEEK, -1, GETDATE());

	========================
	Results
	========================
	Period		RowCounter
	No filter	82,525,715
	1 Year		45,348,323
	6 Months	17,207,840 --15,935,904
	3 Months	7,386,530
	1 Month		2,291,047
	1 Week		9,631
***********************************************************************************/

	--DECLARE @MergeResults TABLE
	--(
	--    MergeAction NVARCHAR(10),
	--    InsertedID BIGINT NULL,
	--    DeletedID BIGINT NULL
	--);
	
	
	DECLARE @from date = DATEADD(MONTH,-12, CAST(GETDATE() AS date));
	DECLARE @to   date = CAST(GETDATE() AS date);
	IF OBJECT_ID('tempdb..#pd') IS NOT NULL DROP TABLE #pd;
	SELECT
	    pd.Id                   AS PayrollDetailsID,
	    pd.TimeCardID,
	    pd.LineItemTypeId, 
	    pd.UnitID,
	    pd.ExcludeFromExport    AS ExcludeFromPayBill,
	    pd.AdjustedInTimecardID AS AdjustedTimeCardID,
	    pd.PayrollDetailsTypeID,
	    pd.IsEarlyPay,
	    pd.DateWorked           AS ShiftDate,
	    pd.CreatedDate          AS BronzeCreatedDate,
	    pd.ShiftHours,
	    pd.HoursWorked          AS Quantity,
	    pd.Lunch,
	    pd.PayRate              AS Rate,
	    pd.PayGross             AS Amount,
	    pd.ProcessedDate,
	    CAST(pd.ProcessedDate AS date) AS ProcessedDateDate,
	    pd.ProcessedBy          AS ProcessedByUserID,
	    CONVERT(binary(32), HASHBYTES('SHA2_256', LOWER(LTRIM(RTRIM(ISNULL(pd.Comments,'')))))) AS NormalizedCommentHash,
	    pd.IsDeleted
	INTO #pd
	FROM nurses.Billing.PayrollDetails pd WITH (NOLOCK)
	WHERE pd.ProcessedDate >= @from
	  AND pd.ProcessedDate <  DATEADD(DAY,1,@to);
	
	CREATE CLUSTERED INDEX CX_pd_ProcDate_Id ON #pd(ProcessedDateDate, PayrollDetailsID);
	CREATE INDEX IX_pd_TimeCard ON #pd(TimeCardID);
	
	/* Limit TimeCardDetails to relevant cards, then pick latest per card */
	IF OBJECT_ID('tempdb..#tcd_latest') IS NOT NULL DROP TABLE #tcd_latest;
	WITH tcd AS (
	  SELECT tcd.TimeCardId, tcd.StatusID, tcd.CreatedDate
	  FROM nurses.TimeCard.TimeCardDetails tcd WITH (NOLOCK)
	  INNER JOIN (SELECT DISTINCT TimeCardID FROM #pd) k ON k.TimeCardID = tcd.TimeCardId
	),
	ranked AS (
	  SELECT
	      TimeCardId, StatusID, CreatedDate,
	      ROW_NUMBER() OVER (PARTITION BY TimeCardId ORDER BY CreatedDate DESC) AS rn
	  FROM tcd
	)
	SELECT TimeCardId, StatusID
	INTO #tcd_latest
	FROM ranked
	WHERE rn = 1;
	
	CREATE UNIQUE CLUSTERED INDEX CX_tcd_latest ON #tcd_latest(TimeCardId);
	
	/* De-dup comment hashes from slice to shrink join set */
	IF OBJECT_ID('tempdb..#pd_hashes') IS NOT NULL DROP TABLE #pd_hashes;
	SELECT DISTINCT NormalizedCommentHash
	INTO #pd_hashes
	FROM #pd;
	
	CREATE UNIQUE CLUSTERED INDEX CX_pd_hashes ON #pd_hashes(NormalizedCommentHash);
	
	/* Map hashes to PayBill_Comments once */
	IF OBJECT_ID('tempdb..#cmap') IS NOT NULL DROP TABLE #cmap;
	SELECT h.NormalizedCommentHash, c.PayBillCommentID
	INTO #cmap
	FROM #pd_hashes h
	LEFT JOIN reporting.Silver.PayBill_Comments c WITH (NOLOCK)
	  ON c.Hashed_Comments = h.NormalizedCommentHash;
	
	CREATE UNIQUE CLUSTERED INDEX CX_cmap ON #cmap(NormalizedCommentHash);
	
	/* Merge */
	MERGE reporting.Silver.PayBill_PayrollDetails AS target
    USING
    (
		SELECT
		    p.PayrollDetailsID,
		    p.TimeCardID,
		    lit.PayBillLineTypeID,
		    p.UnitID,
		    p.ExcludeFromPayBill,
		    p.AdjustedTimeCardID,
		    CASE WHEN p.PayrollDetailsTypeID = 1 AND ui.CompanyID = 3 THEN 2
		         WHEN p.PayrollDetailsTypeID = 1 THEN 1
		         ELSE 3 END AS PayBillRecordTypeID,
		    CASE WHEN CASE WHEN ui.CompanyID = 3 THEN 2 ELSE 1 END = 2 THEN 1 ELSE 0 END AS IsContraRevenue,
		    p.IsEarlyPay,
		    p.ShiftDate,
		    p.BronzeCreatedDate,
		    TRY_CAST(CASE WHEN CHARINDEX('-', p.ShiftHours) > 1
		                  THEN LEFT(p.ShiftHours, CHARINDEX('-', p.ShiftHours) - 1) END AS time) AS ShiftDateStartTime,
		    TRY_CAST(CASE WHEN CHARINDEX('-', p.ShiftHours) BETWEEN 2 AND LEN(p.ShiftHours)-1
		                  THEN SUBSTRING(p.ShiftHours, CHARINDEX('-', p.ShiftHours) + 1, LEN(p.ShiftHours)) END AS time) AS ShiftDateEndTime,
		    p.Quantity,
		    p.Lunch,
		    p.Rate,
		    p.Amount,
		    ps.PayBillStatusID,
		    DATEADD(HOUR, d.PSTOffset, p.ProcessedDate) AS ProcessedDate,
		    p.ProcessedByUserID,
		    cm.PayBillCommentID,
		    p.IsDeleted,
		    CASE WHEN p.IsDeleted = 0 AND p.ExcludeFromPayBill = 1 THEN 0 ELSE 1 END AS IsReportable,
		    0 AS TestFlag
		FROM #pd p
		LEFT JOIN reporting.PowerBI.Dates d WITH (NOLOCK) ON d.[Date] = p.ProcessedDateDate
		LEFT JOIN nurses.TimeCard.TimeCards tc WITH (NOLOCK) ON tc.Id = p.TimeCardId
		LEFT JOIN #tcd_latest tcd ON tcd.TimeCardId = tc.Id
		LEFT JOIN nurses.Facility.Brands br WITH (NOLOCK) ON tc.BrandId = br.Id
		LEFT JOIN nurses.dbo.UserInfo ui WITH (NOLOCK) ON ui.UserID = tc.NurseId
		LEFT JOIN #cmap cm ON cm.NormalizedCommentHash = p.NormalizedCommentHash
		LEFT JOIN Silver.PayBill_LineItemTypes lit WITH (NOLOCK) ON lit.LineItemTypeID = p.LineItemTypeId AND lit.IsLineItemType = 1
		LEFT JOIN Silver.PayBill_Statuses ps WITH (NOLOCK) ON ps.PayBillStatusTypeID = 1 AND ps.PayBillSourceStatusID = tcd.StatusId
		--AND (GREATEST([PayDetails].[CreatedDate],[PayDetails].[UpdatedDate]) >=  @EarliestDate OR @EarliestDate IS NULL)
    ) AS source
        ON target.[PayrollDetailsID] = source.[PayrollDetailsID] 
    WHEN MATCHED AND 
    (
        ISNULL(target.[TimeCardID], -1)           <> ISNULL(source.[TimeCardID], -1)
        OR ISNULL(target.[PayBillLineTypeID], -1)    <> ISNULL(source.[PayBillLineTypeID], -1)
        OR ISNULL(target.[UnitID], -1)               <> ISNULL(source.[UnitID], -1)
        OR ISNULL(target.[ExcludeFromPayBill], -1)   <> ISNULL(source.[ExcludeFromPayBill], -1)
        OR ISNULL(target.[AdjustedTimeCardID], -1)   <> ISNULL(source.[AdjustedTimeCardID], -1)
        OR ISNULL(target.[PayBillRecordTypeID], '')  <> ISNULL(source.[PayBillRecordTypeID], '')
        OR ISNULL(target.[IsContraRevenue], -1)		 <> ISNULL(source.[IsContraRevenue], -1)
        OR ISNULL(target.[IsEarlyPay], -1)           <> ISNULL(source.[IsEarlyPay], -1)
        OR ISNULL(target.[ShiftDate], '1900-01-01') <> ISNULL(source.[ShiftDate], '1900-01-01')
        OR ISNULL(target.[BronzeCreatedDate], '1900-01-01') <> ISNULL(source.[BronzeCreatedDate], '1900-01-01')
        OR ISNULL(target.[ShiftDateStartTime], '1900-01-01') <> ISNULL(source.[ShiftDateStartTime], '1900-01-01')
        OR ISNULL(target.[ShiftDateEndTime], '1900-01-01')   <> ISNULL(source.[ShiftDateEndTime], '1900-01-01')
        OR ISNULL(target.[Quantity], -1)             <> ISNULL(source.[Quantity], -1)
        OR ISNULL(target.[Lunch], -1)                <> ISNULL(source.[Lunch], -1)
        OR ISNULL(target.[Rate], -1)                 <> ISNULL(source.[Rate], -1)
        OR ISNULL(target.[Amount], -1)               <> ISNULL(source.[Amount], -1)
        OR ISNULL(target.[PayBillStatusID], '')      <> ISNULL(source.[PayBillStatusID], '')
        OR ISNULL(target.[ProcessedDate], '1900-01-01') <> ISNULL(source.[ProcessedDate], '1900-01-01')
        OR ISNULL(target.[ProcessedByUserID], -1)    <> ISNULL(source.[ProcessedByUserID], -1)
        OR ISNULL(target.[PayBillCommentID], -1)     <> ISNULL(source.[PayBillCommentID], -1)
        OR ISNULL(target.[IsDeleted], -1)            <> ISNULL(source.[IsDeleted], -1)
        OR ISNULL(target.[IsReportable], -1)         <> ISNULL(source.[IsReportable], -1)
        OR ISNULL(target.[TestFlag], -1)             <> ISNULL(source.[TestFlag], -1)
    )
    THEN UPDATE SET
         [TimeCardID]          = source.[TimeCardID]
        ,[PayBillLineTypeID]   = source.[PayBillLineTypeID]
        ,[UnitID]              = source.[UnitID]
        ,[ExcludeFromPayBill]  = source.[ExcludeFromPayBill]
        ,[AdjustedTimeCardID]  = source.[AdjustedTimeCardID]
        ,[PayBillRecordTypeID] = source.[PayBillRecordTypeID]
        ,[IsContraRevenue]	   = source.[IsContraRevenue]
        ,[IsEarlyPay]          = source.[IsEarlyPay]
		,[ShiftDate]		   = source.[ShiftDate]
		,[BronzeCreatedDate]   = source.[BronzeCreatedDate]
        ,[ShiftDateStartTime]  = source.[ShiftDateStartTime]
        ,[ShiftDateEndTime]    = source.[ShiftDateEndTime]
        ,[Quantity]            = source.[Quantity]
        ,[Lunch]               = source.[Lunch]
        ,[Rate]                = source.[Rate]
        ,[Amount]              = source.[Amount]
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
             [PayrollDetailsID]
            ,[TimeCardID]
            ,[PayBillLineTypeID]
            ,[UnitID]
            ,[ExcludeFromPayBill]
            ,[AdjustedTimeCardID]
            ,[PayBillRecordTypeID]
			,[IsContraRevenue]
            ,[IsEarlyPay]
			,[ShiftDate]
			,[BronzeCreatedDate]
            ,[ShiftDateStartTime]
            ,[ShiftDateEndTime]
            ,[Quantity]
            ,[Lunch]
            ,[Rate]
            ,[Amount]
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
             source.[PayrollDetailsID]
            ,source.[TimeCardID]
            ,source.[PayBillLineTypeID]
            ,source.[UnitID]
            ,source.[ExcludeFromPayBill]
            ,source.[AdjustedTimeCardID]
            ,source.[PayBillRecordTypeID]
            ,source.[IsEarlyPay]
			,source.[IsContraRevenue]
			,source.[ShiftDate]
			,source.[BronzeCreatedDate]
            ,source.[ShiftDateStartTime]
            ,source.[ShiftDateEndTime]
            ,source.[Quantity]
            ,source.[Lunch]
            ,source.[Rate]
            ,source.[Amount]
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
	--    inserted.[PayrollDetailsID],
	--    deleted.[PayrollDetailsID]
	--INTO @MergeResults;
	
	---- Aggregate:
	--SELECT
	--    MergeAction,
	--    COUNT(InsertedID) AS InsertCount,
	--    COUNT(DeletedID) AS DeleteCount
	--FROM @MergeResults
	--GROUP BY MergeAction
	;

END;
