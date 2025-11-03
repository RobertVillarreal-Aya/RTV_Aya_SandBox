USE [Reporting];
GO

SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROC [Silver].[UpdatePayBill_OtherPayments_Bill]
--(@EarliestDate DATE)
AS
BEGIN
/********************************************************************************** 
EXEC [Silver].[UpdatePayBill_OtherPayments_Bill]
--TRUNCATE TABLE [Silver].[PayBill_OtherPayments_Bill]
SELECT top 10000 * FROM [Silver].[PayBill_OtherPayments_Bill]

/*
====DELETE THIS AFTER USE======
EXEC [Temp].[RobertV_UpdatePerformanceCheck]
    @SprocName = 'Silver.UpdatePayBill_OtherPayments_Bill',
    @DoTruncate = 1,
    @DateParam = '@LoadDate';
*/

Change Log:
Date        Author        Performance (rows/time)                                                               Ticket - notes
----------  ---------     ------------------------------------------------------                                ---------------------------
10/16/2025	Robert V	 Initial: Rows 486048 | Time: 0m 7s   ||   Incremental: Rows Added 0 | Time: 0m 18s		TicketNum - Sproc Creation
10/31/2025	Robert V	 Initial: Rows 505828 | Time: 0m 4s   ||   Incremental: Rows Added 0 | Time: 0m 4s		Updated Architecture 

***********************************************************************************/

	--DECLARE @MergeResults TABLE
	--(
	--    MergeAction NVARCHAR(10),
	--    InsertedID BIGINT NULL,
	--    DeletedID BIGINT NULL
	--);
    MERGE reporting.Silver.PayBill_OtherPayments_Bill AS target
    USING
    (
		SELECT --TOP 1000000
		         [OtherPayments].Id                               AS [OtherPaymentID]
		        ,[OtherPayments].[TimeCardID]
		        ,[lit].[PayBillLineTypeID]                        AS [PayBillLineTypeID]
		        ,[OtherPayments].[UnitID]
		        ,[OtherPayments].[IsExcludedFromBilling]          AS [BillHold]
		        ,[OtherPayments].[AdjustedInTimecardID]           AS [AdjustedTimeCardID]
		        ,3                                                AS [PayBillRecordTypeID]
		        ,0                                                AS [IsContraRevenue]
		        ,[OtherPayments].[IsEarlyPay]
		        ,TRY_CAST([OtherPayments].AppliedForDate AS DATE) AS [ShiftDate]
		        ,TRY_CAST([OtherPayments].CreatedDate   AS DATE)  AS [BronzeCreatedDate]
		        ,[OtherPayments].[BillAmount]                     AS [Amount]
		        ,ps.PayBillStatusID
		        ,TRY_CAST(DATEADD(HOUR, d.PSTOffset, [OtherPayments].[ClientInvoiceProcessedDate]) AS DATETIME) AS [ProcessedDate]
		        ,[OtherPayments].[ProcessedBy]                    AS [ProcessedByUserID]
		        ,c.PayBillCommentID                               AS [PayBillCommentID]
		        ,[OtherPayments].[IsDeleted]
		        ,CASE WHEN [OtherPayments].[IsDeleted] = 0 AND [OtherPayments].[IsExcludedFromBilling] = 1 THEN 0 ELSE 1 END AS [IsReportable]
		        ,0                                                AS [TestFlag]
		        ,CASE
		            WHEN [OtherPayments].CreatedBy = 'c7a2cb0f-7738-4122-9008-7ee30802d8f4'
		                 AND CAST([OtherPayments].CreatedDate AS DATETIME) IN (
		                       '2024-01-10T22:25:00'
		                      ,'2024-01-29T22:15:00'
		                      ,'2024-02-12T22:29:00'
		                      ,'2024-08-30T09:12:00')
		              THEN 1
		            WHEN [OtherPayments].Comments LIKE '%migration%' THEN 1
		            WHEN [OtherPayments].CreatedBy = 'cbe6b5bb-2de5-40d8-9a8d-08dc37c16b22' THEN 1
		            ELSE 0
		         END AS [MigrationFlag]
		FROM [nurses].[Billing].[OtherPayments] AS [OtherPayments] WITH (NOLOCK)
		LEFT JOIN reporting.PowerBI.Dates d WITH (NOLOCK) ON d.[Date]=CONVERT(DATE,[OtherPayments].[ClientInvoiceProcessedDate])
		LEFT JOIN nurses.TimeCard.TimeCards tc WITH (NOLOCK) ON tc.id = [OtherPayments].TimeCardId
		LEFT JOIN nurses.[Billing].[PaymentTypes] [PaymentTypes] WITH (NOLOCK) ON [PaymentTypes].[Id] = [OtherPayments].[PaymentTypeId]
		LEFT JOIN reporting.Silver.PayBill_Comments AS c WITH (NOLOCK) ON c.Hashed_Comments = CONVERT(BINARY(32), HASHBYTES('SHA2_256', LTRIM(RTRIM(LOWER([OtherPayments].[Comments])))))
		LEFT JOIN [Silver].[PayBill_LineItemTypes] lit WITH (NOLOCK) ON lit.[PaymentTypeId] = [OtherPayments].[PaymentTypeId]
		LEFT JOIN [Silver].[PayBill_Statuses] ps WITH (NOLOCK) ON ps.PayBillStatusTypeID = 5 AND ps.PayBillSourceStatusID = CONVERT(INT, [OtherPayments].[IsApproved])
		WHERE 0 = 0
		  AND (
		        ([OtherPayments].BillAmount <> 0)
		       OR
		        (1 = 1
		         AND [OtherPayments].BillAmount = 0
		         AND [PaymentTypes].IsBillable = 1
		         AND ([OtherPayments].IsSystemGenerated = 0 OR [OtherPayments].ignoretimecardchanges = 1))
		      )

    ) AS source
        ON target.[OtherPaymentID] = source.[OtherPaymentID] 
    WHEN MATCHED AND 
    (
         ISNULL(target.[TimeCardID], -1)				<> ISNULL(source.[TimeCardID], -1)
        OR ISNULL(target.[PayBillLineTypeID], 0)		<> ISNULL(source.[PayBillLineTypeID], 0)
        OR ISNULL(target.[UnitID], -1)					<> ISNULL(source.[UnitID], -1)
        OR ISNULL(target.[BillHold], -1)					<> ISNULL(source.[BillHold], -1)
        OR ISNULL(target.[AdjustedTimeCardID], -1)		<> ISNULL(source.[AdjustedTimeCardID], -1)
        OR ISNULL(target.[PayBillRecordTypeID], -1)		<> ISNULL(source.[PayBillRecordTypeID], -1)
        OR ISNULL(target.[IsContraRevenue], -1)			<> ISNULL(source.[IsContraRevenue], -1)
        OR ISNULL(target.[IsEarlyPay], -1)				<> ISNULL(source.[IsEarlyPay], -1)
        OR ISNULL(target.[ShiftDate], '')				<> ISNULL(source.[ShiftDate], '')
        OR ISNULL(target.[BronzeCreatedDate], '1900-01-01')		<> ISNULL(source.[BronzeCreatedDate], '1900-01-01')
        OR ISNULL(target.[Amount], -1)					<> ISNULL(source.[Amount], -1)
        OR ISNULL(target.[MigrationFlag], -1)			<> ISNULL(source.[MigrationFlag], -1)
        OR ISNULL(target.[PayBillStatusID], -1)			<> ISNULL(source.[PayBillStatusID], -1)
        OR ISNULL(target.[ProcessedDate], '')			<> ISNULL(source.[ProcessedDate], '')
        OR ISNULL(target.[ProcessedByUserID], -1)		<> ISNULL(source.[ProcessedByUserID], -1)
        OR ISNULL(target.[PayBillCommentID], -1)		<> ISNULL(source.[PayBillCommentID], -1)
        OR ISNULL(target.[IsDeleted], -1)				<> ISNULL(source.[IsDeleted], -1)
        OR ISNULL(target.[IsReportable], -1)			<> ISNULL(source.[IsReportable], -1)
        --OR ISNULL(target.[TestFlag], -1)				<> ISNULL(source.[TestFlag], -1)
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

    WHEN NOT MATCHED BY target THEN
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

    WHEN NOT MATCHED BY source THEN 
        DELETE;
--	OUTPUT
--	    $action AS MergeAction,
--	    inserted.[OtherPaymentID],
--	    deleted.[OtherPaymentID]
--	INTO @MergeResults;
	
--	-- Aggregate:
--	SELECT
--	    MergeAction,
--	    COUNT(InsertedID) AS InsertCount,
--	    COUNT(DeletedID) AS DeleteCount
--	FROM @MergeResults
--	GROUP BY MergeAction;
-------------------------------------------------------------------------------------------------------------
		 --$action AS [MergeAction],
		 --inserted.[OtherPaymentID],
		 --CASE WHEN ISNULL(deleted.[TimeCardID], -1) <> ISNULL(inserted.[TimeCardID], -1) THEN 'TimeCardID ' ELSE '' END +
		 --CASE WHEN ISNULL(deleted.[PayBillLineTypeID], -1) <> ISNULL(inserted.[PayBillLineTypeID], -1) THEN 'PayBillLineTypeID ' ELSE '' END +
		 --CASE WHEN ISNULL(deleted.[UnitID], -1) <> ISNULL(inserted.[UnitID], -1) THEN 'UnitID ' ELSE '' END +
		 --CASE WHEN ISNULL(deleted.[BillHold], -1) <> ISNULL(inserted.[BillHold], -1) THEN 'BillHold ' ELSE '' END +
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