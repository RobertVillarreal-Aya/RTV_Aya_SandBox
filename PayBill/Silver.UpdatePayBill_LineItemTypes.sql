USE [Reporting];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
 
CREATE OR ALTER PROC [Silver].[UpdatePayBill_LineItemTypes]
AS
BEGIN
/***********************************************************************************************
EXEC reporting.Silver.UpdatePayBill_LineItemTypes
--TRUNCATE TABLE reporting.Silver.PayBill_LineItemTypes

Change Log:
Date        Author        Performance (rows/time)                                                               Ticket - notes
----------  ---------     ------------------------------------------------------                                ---------------------------
10/16/2025	Robert V	 Initial: Rows 95 | Time: 0m 0s   ||   Incremental: Rows Added 0 | Time: 0m 0s			TicketNum - Sproc Creation

***********************************************************************************************/


    IF OBJECT_ID(N'TEMPDB.dbo.#AllTypes', N'U') IS NOT NULL DROP TABLE #AllTypes;

    SELECT Description 
    INTO #AllTypes
    FROM nurses.billing.LineItemTypes
    UNION
    SELECT Name FROM nurses.billing.PaymentTypes;

    MERGE [Reporting].[Silver].[PayBill_LineItemTypes] AS target
    USING
    (
        SELECT 
              a.Description                                       AS [PayBillLineTypeDescription]
            , ISNULL(lit.Id, pt.BillingLineItemTypeId)            AS [LineItemTypeID]
            , CASE WHEN lit.Id IS NULL THEN 0 ELSE 1 END          AS [IsLineItemType]
            , pt.Id                                               AS [PaymentTypeID]
            , ISNULL(ct.Type, pt.Type)                            AS [BillCategory]
            , ISNULL(pt.Type, ct.Type)                            AS [PayCategory]
            , ISNULL(pt.DayforceXrefCode, lit.DayforceXrefCode)   AS [DayforceXrefCode]
            , ISNULL(pt.DayforceLocumsXrefCode, lit.DayforceLocumsXrefCode)
                                                                 AS [DayforceLocumsXrefCode]
            , pt.DisplayOrder                                     AS [PaymentTypeSortOrder]
            , pt.IsTaxable                                        AS [PaymentTypeIsTaxable]
            , pt.IsBillable                                       AS [PaymentTypeIsBillable]
            , pt.IsPayable                                        AS [PaymentTypeIsPayable]
            , pt.ContractSubTypeId
            , cest.contractExtra_subtypename                      AS [ContractSubType]
            , cest.contractExtra_subtypecode                      AS [ContractSubTypeCode]
            , ept.ExternalPaymentTypeNameId
            , ept.Name                                            AS [ExternalPaymentTypeName]
        FROM #AllTypes a
        LEFT JOIN nurses.billing.LineItemTypes lit 
            ON lit.Description = a.Description
        LEFT JOIN nurses.billing.PaymentTypes pt 
            ON pt.Name = a.Description
        LEFT JOIN nurses.billing.CategoryTypes ct 
            ON lit.CategoryId = ct.Id
        LEFT JOIN nurses.billing.ExternalPaymentTypeNames ept 
            ON ept.PaymentTypeId = pt.Id
        LEFT JOIN nurses.dbo.contractExtraSubTypes cest 
            ON cest.contractExtra_subtype_id = pt.ContractSubTypeId
    ) AS source
        ON target.PayBillLineTypeDescription = source.PayBillLineTypeDescription

    WHEN MATCHED AND (
           ISNULL(target.LineItemTypeID, 0)             <> ISNULL(source.LineItemTypeID, 0)
        OR ISNULL(target.IsLineItemType, 0)             <> ISNULL(source.IsLineItemType, 0)
        OR ISNULL(target.PaymentTypeID, 0)              <> ISNULL(source.PaymentTypeID, 0)
        OR ISNULL(target.BillCategory, '')              <> ISNULL(source.BillCategory, '')
        OR ISNULL(target.PayCategory, '')               <> ISNULL(source.PayCategory, '')
        OR ISNULL(target.DayforceXrefCode, '')           <> ISNULL(source.DayforceXrefCode, '')
        OR ISNULL(target.DayforceLocumsXrefCode, '')     <> ISNULL(source.DayforceLocumsXrefCode, '')
        OR ISNULL(target.PaymentTypeSortOrder, 0)       <> ISNULL(source.PaymentTypeSortOrder, 0)
        OR ISNULL(target.PaymentTypeIsTaxable, 0)       <> ISNULL(source.PaymentTypeIsTaxable, 0)
        OR ISNULL(target.PaymentTypeIsBillable, 0)      <> ISNULL(source.PaymentTypeIsBillable, 0)
        OR ISNULL(target.PaymentTypeIsPayable, 0)       <> ISNULL(source.PaymentTypeIsPayable, 0)
        OR ISNULL(target.ContractSubTypeId, 0)          <> ISNULL(source.ContractSubTypeId, 0)
        OR ISNULL(target.ContractSubType, '')           <> ISNULL(source.ContractSubType, '')
        OR ISNULL(target.ContractSubTypeCode, '')       <> ISNULL(source.ContractSubTypeCode, '')
        OR ISNULL(target.ExternalPaymentTypeNameId, 0)  <> ISNULL(source.ExternalPaymentTypeNameId, 0)
        OR ISNULL(target.ExternalPaymentTypeName, '')   <> ISNULL(source.ExternalPaymentTypeName, '')
    )

    THEN UPDATE SET
          target.LineItemTypeID            = source.LineItemTypeID
        , target.IsLineItemType            = source.IsLineItemType
        , target.PaymentTypeID             = source.PaymentTypeID
        , target.BillCategory              = source.BillCategory
        , target.PayCategory               = source.PayCategory
        , target.DayforceXrefCode          = source.DayforceXrefCode
        , target.DayforceLocumsXrefCode    = source.DayforceLocumsXrefCode
        , target.PaymentTypeSortOrder      = source.PaymentTypeSortOrder
        , target.PaymentTypeIsTaxable      = source.PaymentTypeIsTaxable
        , target.PaymentTypeIsBillable     = source.PaymentTypeIsBillable
        , target.PaymentTypeIsPayable      = source.PaymentTypeIsPayable
        , target.ContractSubTypeId         = source.ContractSubTypeId
        , target.ContractSubType           = source.ContractSubType
        , target.ContractSubTypeCode       = source.ContractSubTypeCode
        , target.ExternalPaymentTypeNameId = source.ExternalPaymentTypeNameId
        , target.ExternalPaymentTypeName   = source.ExternalPaymentTypeName

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
              [PayBillLineTypeDescription]
            , [LineItemTypeID]
            , [IsLineItemType]
            , [PaymentTypeID]
            , [BillCategory]
            , [PayCategory]
            , [DayforceXrefCode]
            , [DayforceLocumsXrefCode]
            , [PaymentTypeSortOrder]
            , [PaymentTypeIsTaxable]
            , [PaymentTypeIsBillable]
            , [PaymentTypeIsPayable]
            , [ContractSubTypeId]
            , [ContractSubType]
            , [ContractSubTypeCode]
            , [ExternalPaymentTypeNameId]
            , [ExternalPaymentTypeName]
        )
        VALUES (
              source.[PayBillLineTypeDescription]
            , source.[LineItemTypeID]
            , source.[IsLineItemType]
            , source.[PaymentTypeID]
            , source.[BillCategory]
            , source.[PayCategory]
            , source.[DayforceXrefCode]
            , source.[DayforceLocumsXrefCode]
            , source.[PaymentTypeSortOrder]
            , source.[PaymentTypeIsTaxable]
            , source.[PaymentTypeIsBillable]
            , source.[PaymentTypeIsPayable]
            , source.[ContractSubTypeId]
            , source.[ContractSubType]
            , source.[ContractSubTypeCode]
            , source.[ExternalPaymentTypeNameId]
            , source.[ExternalPaymentTypeName]
        );


END;
GO
