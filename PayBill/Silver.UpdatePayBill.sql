USE [Reporting];
GO

SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROC [Silver].[UpdatePayBill]
AS BEGIN
/********************************************************************************** 
EXEC [Silver].[UpdatePayBill]
--TRUNCATE TABLE [Silver].[PayBill]
SELECT top 10000 * FROM [Silver].[PayBill]

Change Log:
Date        Author        Performance (rows/time)																	Ticket - notes
----------  ---------     ------------------------------------------------------									---------------------------
10/16/2025	Robert V	 Initial: Rows 93,741,160 | Time: 8m 8s   ||   Incremental: Rows Added 0 | Time: 0m 18s		TicketNum - Sproc Creation
***********************************************************************************/
	MERGE [reporting].[Silver].[PayBill] AS [Target]
	USING
	(
	    SELECT 
	          [PayDetails].[Id] AS PayrollSourceID --82,525,669
			 ,PayBillTypeID = 0
	    FROM [nurses].[Billing].[PayrollDetails] AS [PayDetails] WITH (NOLOCK)
		WHERE 0 = 0 

	UNION

	    SELECT 
	         [OtherPayments].[Id]
	        ,PayBillTypeID = 1
	    FROM [nurses].[Billing].[OtherPayments] AS [OtherPayments] WITH (NOLOCK)
			LEFT JOIN nurses.[Billing].[PaymentTypes] [PaymentTypes] WITH (NOLOCK) ON [PaymentTypes].[Id] = [OtherPayments].[Id]		
		WHERE 0 = 0 
 
	UNION

	    SELECT 
	         [OtherPayments].[Id]
	        ,PayBillTypeID = 2
	    FROM [nurses].[Billing].[OtherPayments] AS [OtherPayments] WITH (NOLOCK)
			LEFT JOIN nurses.[Billing].[PaymentTypes] [PaymentTypes] WITH (NOLOCK) ON [PaymentTypes].[Id] = [OtherPayments].[Id]		
		WHERE 0 = 0
		AND
			(
				(
				[OtherPayments].BillAmount <> 0
				)
			OR
				(
				1=1
				AND [OtherPayments].BillAmount = 0 
				AND [PaymentTypes].IsBillable = 1
				AND 
					(
					[OtherPayments].IsSystemGenerated = 0 OR [OtherPayments].ignoretimecardchanges = 1
					)
				)
			)

	) AS [Source]
	    ON [Target].PayrollSourceID = [Source].PayrollSourceID 
			AND [Target].[PayBillTypeID] = [Source].[PayBillTypeID]
	
	-- Insert new rows
	WHEN NOT MATCHED BY TARGET THEN
	    INSERT (PayrollSourceID, [PayBillTypeID])
	    VALUES ([Source].PayrollSourceID, [Source].[PayBillTypeID])
	
	-- Delete rows missing from source
	WHEN NOT MATCHED BY SOURCE THEN
	    DELETE
	;

END 
	