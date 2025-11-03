USE [Reporting];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO


CREATE OR ALTER PROC [Silver].[UpdatePayBill_PayDetails]
AS
BEGIN
/***********************************************************************************************
EXEC [Silver].[UpdatePayBill_PayDetails]
--TRUNCATE TABLE [Silver].[PayBill_PayDetails]
SELECT top 10000 * FROM [Silver].[PayBill_PayDetails]

/*
====DELETE THIS AFTER USE======
EXEC [Temp].[RobertV_UpdatePerformanceCheck]
    @SprocName = 'Silver.UpdatePayBill_PayDetails',
    @DoTruncate = 1,
    @DateParam = '@LoadDate';
*/

Change Log:
Description: Calculates PullWeekID for each PayBill record using workweek boundaries.

Date        Author       Performance (rows/time)																		Ticket - notes
----------  ---------    ------------------------------------------------------											---------------------------
10/31/2025	Robert V	 Initial: Rows 323,52,886 | Time: 3m 15s   ||   Incremental: Rows Added 0 | Time: 1m 22s		TicketNum - Sproc Creation
***********************************************************************************************/
--    DECLARE @MergeResults TABLE
--    (
--        MergeAction NVARCHAR(10),
--        InsertedID BIGINT NULL,
--        DeletedID BIGINT NULL
--    );

	DECLARE @today date = CAST(GETDATE() AS date);
	
	;WITH p AS (
	  SELECT
	      p.paybillid,
	      CAST(p.processeddate AS date) AS ProcessedDateDate,
	      CAST(p.shiftdate     AS date) AS ShiftDateDate,
	      p.BronzeCreatedDate
	  FROM reporting.Silver.PayBill_Details p WITH (NOLOCK)
	  WHERE p.ispay = 1
	)
	
    MERGE [Reporting].[Silver].[PayBill_PayDetails] AS target
    USING
    (
		SELECT
		    p.paybillid,
		    CASE 
		      WHEN p.ProcessedDateDate IS NULL THEN
		        CASE 
		          WHEN p.BronzeCreatedDate > cw.WorkWeekToDate AND sw.PayrollWorkWeeksDatesID <= cw.PayrollWorkWeeksDatesID THEN cw.PayrollWorkWeeksDatesID + 1
		          WHEN sw.PayrollWorkWeeksDatesID <= cw.PayrollWorkWeeksDatesID THEN cw.PayrollWorkWeeksDatesID
		          ELSE sw.PayrollWorkWeeksDatesID
		        END
		      ELSE pw.PayrollWorkWeeksDatesID
		    END AS PullWeekID
		FROM p
			OUTER APPLY (SELECT TOP (1) w.PayrollWorkWeeksDatesID, w.WorkWeekFromDate, w.WorkWeekToDate FROM reporting.Silver.PayBill_WorkWeeks w WITH (NOLOCK) WHERE p.ProcessedDateDate BETWEEN w.WorkWeekFromDate AND w.WorkWeekToDate ORDER BY w.WorkWeekFromDate DESC, w.WorkWeekToDate DESC, w.PayrollWorkWeeksDatesID DESC) pw
			OUTER APPLY (SELECT TOP (1) w.PayrollWorkWeeksDatesID, w.WorkWeekFromDate, w.WorkWeekToDate FROM reporting.Silver.PayBill_WorkWeeks w WITH (NOLOCK) WHERE p.ShiftDateDate     BETWEEN w.WorkWeekFromDate AND w.WorkWeekToDate ORDER BY w.WorkWeekFromDate DESC, w.WorkWeekToDate DESC, w.PayrollWorkWeeksDatesID DESC) sw
			OUTER APPLY (SELECT TOP (1) w.PayrollWorkWeeksDatesID, w.WorkWeekFromDate, w.WorkWeekToDate FROM reporting.Silver.PayBill_WorkWeeks w WITH (NOLOCK) WHERE @today              BETWEEN w.WorkWeekFromDate AND w.WorkWeekToDate ORDER BY w.WorkWeekFromDate DESC, w.WorkWeekToDate DESC, w.PayrollWorkWeeksDatesID DESC) cw
    ) AS source
        ON target.PayBillID = source.PayBillID

    WHEN MATCHED AND ISNULL(target.PullWeekID, -1) <> ISNULL(source.PullWeekID, -1)
    THEN UPDATE SET
        target.PullWeekID = source.PullWeekID,
        target.ETL_ModifiedDate = GETDATE()

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (PayBillID, PullWeekID, ETL_CreatedDate, ETL_ModifiedDate)
        VALUES (source.PayBillID, source.PullWeekID, GETDATE(), GETDATE());

END;
GO
