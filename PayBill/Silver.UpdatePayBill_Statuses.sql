USE [Reporting];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROC [Silver].[UpdatePayBill_Statuses]
AS
BEGIN
/***********************************************************************************************
EXEC reporting.Silver.UpdatePayBill_Statuses
--TRUNCATE TABLE reporting.Silver.PayBill_Statuses

Change Log:
Date        Author        Performance (rows/time)                                                               Ticket - notes
----------  ---------     ------------------------------------------------------                                ---------------------------
10/16/2025	Robert V	 Initial: Rows 21 | Time: 0m 0s   ||   Incremental: Rows Added 0 | Time: 0m 0s			TicketNum - Sproc Creation
***********************************************************************************************/

    DECLARE @MergeResults TABLE
    (
        MergeAction NVARCHAR(10),
        InsertedID BIGINT NULL,
        DeletedID BIGINT NULL
    );

    MERGE [Reporting].[Silver].[PayBill_Statuses] AS target
    USING
    (
        SELECT ID AS PayBillSourceStatusID, 1 AS PayBillStatusTypeID, StatusName 
        FROM nurses.timecard.TimeCardStatus
        WHERE Active = 1

        UNION

        SELECT ExpenseStatusId AS PayBillSourceStatusID, 2 AS PayBillStatusTypeID, Name AS StatusName 
        FROM nurses.billing.ExpenseStatuses

        UNION

        SELECT ID AS PayBillSourceStatusID, 3 AS PayBillStatusTypeID, Name AS StatusName 
        FROM nurses.billing.InvoiceStatuses

        UNION

        SELECT ID AS PayBillSourceStatusID, 4 AS PayBillStatusTypeID, Name AS StatusName 
        FROM nurses.billing.ReverseInvoiceStatuses

        UNION

        SELECT 0, 5, 'Unapproved'

        UNION

        SELECT 1, 5, 'Approved'

        UNION

        SELECT 2, 5, 'No Status'
    ) AS source
        ON target.[PayBillSourceStatusID] = source.[PayBillSourceStatusID]
       AND target.[PayBillStatusTypeID] = source.[PayBillStatusTypeID]

    WHEN MATCHED AND (
        ISNULL(target.[StatusName], '') <> ISNULL(source.[StatusName], '')
    )
    THEN UPDATE SET
        target.[StatusName] = source.[StatusName]

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            [PayBillSourceStatusID],
            [PayBillStatusTypeID],
            [StatusName]
        )
        VALUES (
            source.[PayBillSourceStatusID],
            source.[PayBillStatusTypeID],
            source.[StatusName]
        );

END;
GO
