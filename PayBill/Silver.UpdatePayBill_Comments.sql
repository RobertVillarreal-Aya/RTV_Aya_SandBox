USE [reporting]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROC [Silver].[UpdatePayBill_Comments]
AS BEGIN
/****************************************************************************************************************
EXEC reporting.Silver.UpdatePayBill_Comments
--TRUNCATE TABLE reporting.Silver.PayBill_Comments
SELECT max(LEN([Comments])) length FROM reporting.Silver.PayBill_Comments

/*
====DELETE THIS AFTER USE======
EXEC [Temp].[RobertV_UpdatePerformanceCheck]
    @SprocName = 'Silver.UpdatePayBill_Comments',
    @DoTruncate = 1,                        
    @DateParam = '@LoadDate';               
*/

Date        Author        Performance (rows/time)																	Ticket - notes
----------  ---------     ------------------------------------------------------									---------------------------
10/16/2025	Robert V	 Initial: Rows 175138 | Time: 0m 14s   ||   Incremental: Rows Added 0 | Time: 0m 13s		TicketNum - Sproc Creation
****************************************************************************************************************/

--normalized comments
IF OBJECT_ID('tempdb..#Comments') IS NOT NULL DROP TABLE #Comments;

SELECT 
    MIN(OriginalComment) AS [Comments],
    LTRIM(RTRIM(LOWER(OriginalComment))) AS [Normalized_Comments],
    CONVERT(BINARY(32), HASHBYTES('SHA2_256', LTRIM(RTRIM(LOWER(OriginalComment))))) AS [Hashed_Comments]
INTO #Comments
FROM (
    SELECT pd.[Comments] AS OriginalComment
    FROM Nurses.Billing.PayrollDetails AS pd WITH (NOLOCK)
    WHERE pd.[Comments] IS NOT NULL
      AND LTRIM(RTRIM(pd.[Comments])) <> ''

    UNION ALL

    SELECT op.[Comments]
    FROM Nurses.Billing.OtherPayments AS op WITH (NOLOCK)
    WHERE op.[Comments] IS NOT NULL
      AND LTRIM(RTRIM(op.[Comments])) <> ''
) AS src
GROUP BY LTRIM(RTRIM(LOWER(OriginalComment)));

CREATE UNIQUE CLUSTERED INDEX IX_Comments_Hash ON #Comments (Hashed_Comments);

--merge
MERGE Reporting.Silver.PayBill_Comments AS Target
USING #Comments AS Source
ON Target.Hashed_Comments = Source.Hashed_Comments

WHEN NOT MATCHED BY TARGET THEN
    INSERT ([Comments], [Hashed_Comments])
    VALUES (Source.[Comments], Source.[Hashed_Comments])

WHEN NOT MATCHED BY SOURCE THEN
    DELETE;




END