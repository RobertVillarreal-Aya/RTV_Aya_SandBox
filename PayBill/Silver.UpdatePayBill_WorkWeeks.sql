USE [Reporting];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROC [Silver].[UpdatePayBill_WorkWeeks]
AS
BEGIN
/***********************************************************************************************
EXEC reporting.Silver.UpdatePayBill_WorkWeeks
--TRUNCATE TABLE reporting.Silver.PayBill_WorkWeeks

Change Log:
Date          Author         Performance (rows/time)																Ticket - notes    
------------  -------------- ------------------------------------------------------									---------------------------
10/14/2025	  Robert V		 Initial: Rows 423 | Time(m): 0.01   ||   Incremental: Rows Added 0 | Time(m): 0.00		TicketNum - Sproc Creation
***********************************************************************************************/

    DECLARE @MergeResults TABLE
    (
        MergeAction NVARCHAR(10),
        InsertedID INT NULL,
        DeletedID INT NULL
    );

    MERGE [Reporting].[Silver].[PayBill_WorkWeeks] AS target
    USING
    (
        SELECT 
            pww.Id AS PayrollWorkWeeksDatesID,
            pww.Year AS WorkWeekYear,
            pww.WorkWeekNumber,
            CAST(DATEADD(WK, DATEDIFF(WK, 6, IIF(DATEPART(WEEKDAY, pww.FromDateTime) = 1, DATEADD(DAY, -1, pww.FromDateTime), pww.FromDateTime)), 6) AS DATE) AS WorkWeekFromDate,
            CAST(DATEADD(WK, DATEDIFF(WK, 5, IIF(DATEPART(WEEKDAY, pww.FromDateTime) = 1, DATEADD(DAY, -1, pww.FromDateTime), pww.FromDateTime)), 5) AS DATE) AS WorkWeekToDate,
            CAST(pww.FromDateTime AS DATE) AS PayrollWorkWeekFromDate,
            CAST(pww.ToDateTime AS DATE) AS PayrollWorkWeekToDate,
            DATEADD(DAY, 1, CAST(pww.ToDateTime AS DATE)) AS PayrollProcessingDate,
            pww.IsBiWeeklyRegular
        FROM [nurses].[Timecard].[PayrollWorkWeekDates] pww
    ) AS source
        ON target.[PayrollWorkWeeksDatesID] = source.[PayrollWorkWeeksDatesID]

    WHEN MATCHED AND (
           ISNULL(target.[WorkWeekYear], -1) <> ISNULL(source.[WorkWeekYear], -1)
        OR ISNULL(target.[WorkWeekNumber], -1) <> ISNULL(source.[WorkWeekNumber], -1)
        OR ISNULL(target.[WorkWeekFromDate], '1900-01-01') <> ISNULL(source.[WorkWeekFromDate], '1900-01-01')
        OR ISNULL(target.[WorkWeekToDate], '1900-01-01') <> ISNULL(source.[WorkWeekToDate], '1900-01-01')
        OR ISNULL(target.[PayrollWorkWeekFromDate], '1900-01-01') <> ISNULL(source.[PayrollWorkWeekFromDate], '1900-01-01')
        OR ISNULL(target.[PayrollWorkWeekToDate], '1900-01-01') <> ISNULL(source.[PayrollWorkWeekToDate], '1900-01-01')
        OR ISNULL(target.[PayrollProcessingDate], '1900-01-01') <> ISNULL(source.[PayrollProcessingDate], '1900-01-01')
        OR ISNULL(target.[IsBiWeeklyRegular], 0) <> ISNULL(source.[IsBiWeeklyRegular], 0)
    )
    THEN UPDATE SET
        target.[WorkWeekYear] = source.[WorkWeekYear],
        target.[WorkWeekNumber] = source.[WorkWeekNumber],
        target.[WorkWeekFromDate] = source.[WorkWeekFromDate],
        target.[WorkWeekToDate] = source.[WorkWeekToDate],
        target.[PayrollWorkWeekFromDate] = source.[PayrollWorkWeekFromDate],
        target.[PayrollWorkWeekToDate] = source.[PayrollWorkWeekToDate],
        target.[PayrollProcessingDate] = source.[PayrollProcessingDate],
        target.[IsBiWeeklyRegular] = source.[IsBiWeeklyRegular]

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            [PayrollWorkWeeksDatesID],
            [WorkWeekYear],
            [WorkWeekNumber],
            [WorkWeekFromDate],
            [WorkWeekToDate],
            [PayrollWorkWeekFromDate],
            [PayrollWorkWeekToDate],
            [PayrollProcessingDate],
            [IsBiWeeklyRegular]
        )
        VALUES (
            source.[PayrollWorkWeeksDatesID],
            source.[WorkWeekYear],
            source.[WorkWeekNumber],
            source.[WorkWeekFromDate],
            source.[WorkWeekToDate],
            source.[PayrollWorkWeekFromDate],
            source.[PayrollWorkWeekToDate],
            source.[PayrollProcessingDate],
            source.[IsBiWeeklyRegular]
        );

    --OUTPUT $action, inserted.[PayrollWorkWeeksDatesID], deleted.[PayrollWorkWeeksDatesID]
    --INTO @MergeResults;
    --SELECT MergeAction, COUNT(*) FROM @MergeResults GROUP BY MergeAction;

END;
GO
