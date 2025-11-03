USE [Reporting];
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO
/* ==========================================================
   DROP ALL Silver.PayBill TABLES (except Facilities)
   ========================================================== 
IF OBJECT_ID(N'Silver.PayBill_Types','U') IS NOT NULL DROP TABLE Silver.PayBill_Types;
IF OBJECT_ID(N'Silver.PayBill_RecordTypes','U') IS NOT NULL DROP TABLE Silver.PayBill_RecordTypes;
IF OBJECT_ID(N'Silver.PayBill_StatusTypes','U') IS NOT NULL DROP TABLE Silver.PayBill_StatusTypes;
IF OBJECT_ID(N'Silver.PayBill_Statuses','U') IS NOT NULL DROP TABLE Silver.PayBill_Statuses;
IF OBJECT_ID(N'Silver.PayBill_LineItemTypes','U') IS NOT NULL DROP TABLE Silver.PayBill_LineItemTypes;
IF OBJECT_ID(N'Silver.PayBill_WorkWeeks','U') IS NOT NULL DROP TABLE Silver.PayBill_WorkWeeks;
IF OBJECT_ID(N'Silver.PayBill_Timecards','U') IS NOT NULL DROP TABLE Silver.PayBill_Timecards;
IF OBJECT_ID(N'Silver.PayBill_PayrollDetails','U') IS NOT NULL DROP TABLE Silver.PayBill_PayrollDetails;
IF OBJECT_ID(N'Silver.PayBill_OtherPayments_Pay','U') IS NOT NULL DROP TABLE Silver.PayBill_OtherPayments_Pay;
IF OBJECT_ID(N'Silver.PayBill_OtherPayments_Bill','U') IS NOT NULL DROP TABLE Silver.PayBill_OtherPayments_Bill;
IF OBJECT_ID(N'Silver.PayBill_Comments','U') IS NOT NULL DROP TABLE Silver.PayBill_Comments;
IF OBJECT_ID(N'Silver.PayBill','U') IS NOT NULL DROP TABLE Silver.PayBill;
*/

/* ==========================================================
   EXECUTE ALL Silver.UpdatePayBill STORED PROCEDURES
   (Ordered from Longest to Shortest Expected Runtime)
   ========================================================== 
	--Largest 
	--TRUNCATE TABLE [Silver].[PayBill_PayrollDetails]
	EXEC [Silver].[UpdatePayBill_PayrollDetails];
	PRINT 'Completed: UpdatePayBill_PayrollDetails';

	--TRUNCATE TABLE [Silver].[PayBill] 
	EXEC [Silver].[UpdatePayBill];
	PRINT 'Completed: UpdatePayBill';
	
	-- Medium 
	--TRUNCATE TABLE [Silver].[PayBill_OtherPayments_Bill]
	EXEC [Silver].[UpdatePayBill_OtherPayments_Bill];
	PRINT 'Completed: UpdatePayBill_OtherPayments_Bill';
	
	--TRUNCATE TABLE [Silver].[PayBill_OtherPayments_Pay]
	EXEC [Silver].[UpdatePayBill_OtherPayments_Pay];
	PRINT 'Completed: UpdatePayBill_OtherPayments_Pay';
	
	-- Small
	EXEC [Silver].[UpdatePayBill_WorkWeeks];
	PRINT 'Completed: UpdatePayBill_WorkWeeks';
	
	EXEC [Silver].[UpdatePayBill_Timecards];
	PRINT 'Completed: UpdatePayBill_Timecards';
	
	EXEC [Silver].[UpdatePayBill_LineItemTypes];
	PRINT 'Completed: UpdatePayBill_LineItemTypes';
	
	EXEC [Silver].[UpdatePayBill_Comments];
	PRINT 'Completed: UpdatePayBill_Comments';
	
	EXEC [Silver].[UpdatePayBill_Statuses];
	PRINT 'Completed: UpdatePayBill_Statuses';
GO
*/


/* ==========================================================
   1. Silver.PayBill_Types
   ========================================================== */
IF OBJECT_ID(N'Silver.PayBill_Types','U') IS NULL
BEGIN
    CREATE TABLE Silver.PayBill_Types(
        PayBillTypeID TINYINT NOT NULL PRIMARY KEY,
        PayBillType   VARCHAR(50),
        ETL_CreatedDate DATE NOT NULL DEFAULT GETDATE()
    );
END;
IF NOT EXISTS (SELECT 1 FROM Silver.PayBill_Types)
BEGIN
    INSERT INTO Silver.PayBill_Types(PayBillTypeID,PayBillType)
    VALUES (0,'PayrollDetails'),(1,'OtherPayments Pay Side'),(2,'OtherPayments Bill Side');
END;
GO

/* ==========================================================
   2. Silver.PayBill_RecordTypes
   ========================================================== */
IF OBJECT_ID(N'Silver.PayBill_RecordTypes','U') IS NULL
BEGIN
    CREATE TABLE Silver.PayBill_RecordTypes(
        PayBillRecordTypeID INT PRIMARY KEY,
        PayBillRecordType VARCHAR(100) NOT NULL,
        PayBillRecordTypeClass VARCHAR(50) NOT NULL,
        IsPay BIT NOT NULL DEFAULT 0,
        IsBill BIT NOT NULL DEFAULT 0,
        IsIntercompany BIT NOT NULL DEFAULT 0
    );
END;
IF NOT EXISTS (SELECT 1 FROM Silver.PayBill_RecordTypes)
BEGIN
    INSERT INTO Silver.PayBill_RecordTypes VALUES
    (1,'Payroll','TransactionType',1,0,0),
    (2,'Reverse Invoice','TransactionType',1,0,0),
    (3,'Invoice','TransactionType',0,1,0),
    (4,'Aya Payroll','TimecardType',1,0,1),
    (5,'Lotus Billing','TimecardType',0,1,1),
    (6,'Non-Intercompany','TimecardType',1,1,0);
END;
GO

/* ==========================================================
   3. Silver.PayBill_StatusTypes
   ========================================================== */
IF OBJECT_ID(N'Silver.PayBill_StatusTypes','U') IS NULL
BEGIN
    CREATE TABLE Silver.PayBill_StatusTypes(
        PayBillStatusTypeID INT PRIMARY KEY,
        StatusType VARCHAR(20)
    );
END;
IF NOT EXISTS (SELECT 1 FROM Silver.PayBill_StatusTypes)
BEGIN
    INSERT INTO Silver.PayBill_StatusTypes VALUES
    (1,'TimeCard'),(2,'Expenses'),(3,'Invoice'),(4,'Reverse Invoice'),(5,'Manual');
END;
GO

/* ==========================================================
   4. Silver.PayBill_Statuses
   ========================================================== */
IF OBJECT_ID(N'Silver.PayBill_Statuses','U') IS NULL
BEGIN
    CREATE TABLE Silver.PayBill_Statuses(
        PayBillStatusID INT IDENTITY(1,1) PRIMARY KEY,
        PayBillSourceStatusID INT NOT NULL,
        PayBillStatusTypeID INT NOT NULL,
        StatusName VARCHAR(50)
    );
END;
IF NOT EXISTS(SELECT 1 FROM Silver.PayBill_Statuses)
BEGIN
    INSERT INTO Silver.PayBill_Statuses(PayBillSourceStatusID,PayBillStatusTypeID,StatusName)
    SELECT ID,1,StatusName FROM nurses.timecard.TimeCardStatus WHERE Active=1
    UNION ALL
    SELECT ExpenseStatusId,2,Name FROM nurses.billing.ExpenseStatuses
    UNION ALL
    SELECT ID,3,Name FROM nurses.billing.InvoiceStatuses
    UNION ALL
    SELECT ID,4,Name FROM nurses.billing.ReverseInvoiceStatuses
    UNION ALL
    SELECT 0,5,'Unapproved' UNION ALL
    SELECT 1,5,'Approved'   UNION ALL
    SELECT 2,5,'No Status';
END;
GO

/* ==========================================================
   5. Silver.PayBill_LineItemTypes
   ========================================================== */
IF OBJECT_ID(N'Silver.PayBill_LineItemTypes','U') IS NULL
BEGIN
    CREATE TABLE Silver.PayBill_LineItemTypes(
        PayBillLineTypeID INT IDENTITY(1,1) PRIMARY KEY,
        PayBillLineTypeDescription VARCHAR(100) NOT NULL,
        LineItemTypeID INT,
        IsLineItemType BIT,
        PaymentTypeID INT,
        BillCategory VARCHAR(50),
        PayCategory VARCHAR(50),
        DayforceXrefCode VARCHAR(20),
        DayforceLocumsXrefCode VARCHAR(20),
        PaymentTypeSortOrder INT,
        PaymentTypeIsTaxable BIT,
        PaymentTypeIsBillable BIT,
        PaymentTypeIsPayable BIT,
        ContractSubTypeId INT,
        ContractSubType VARCHAR(50),
        ContractSubTypeCode VARCHAR(20),
        ExternalPaymentTypeNameId INT,
        ExternalPaymentTypeName VARCHAR(50)
    );
END;
GO

/* ==========================================================
   6. Silver.PayBill
   ========================================================== */
IF OBJECT_ID(N'Silver.PayBill','U') IS NULL
BEGIN
    CREATE TABLE Silver.PayBill(
        PayBillID BIGINT IDENTITY(1,1) PRIMARY KEY,
        PayBillTypeID TINYINT NOT NULL,
        PayrollSourceID BIGINT NOT NULL,
        CONSTRAINT UQ_PayBill_Type_Source UNIQUE(PayBillTypeID,PayrollSourceID)
    );
END;
GO

/* ==========================================================
   7. Silver.PayBill_WorkWeeks
   ========================================================== */
IF OBJECT_ID(N'Silver.PayBill_WorkWeeks','U') IS NULL
BEGIN
    CREATE TABLE Silver.PayBill_WorkWeeks(
        PayrollWorkWeeksDatesID INT PRIMARY KEY,
        WorkWeekYear INT NOT NULL,
        WorkWeekNumber INT NOT NULL,
        WorkWeekFromDate DATE NOT NULL,
        WorkWeekToDate DATE NOT NULL,
        PayrollWorkWeekFromDate DATE NOT NULL,
        PayrollWorkWeekToDate DATE NOT NULL,
        PayrollProcessingDate DATE NOT NULL,
        IsBiWeeklyRegular BIT NOT NULL
    );
CREATE INDEX IX_WorkWeeks_From_To 
ON reporting.Silver.PayBill_WorkWeeks(WorkWeekFromDate, WorkWeekToDate) 
INCLUDE (PayrollWorkWeeksDatesID);
END;
IF NOT EXISTS (SELECT 1 FROM Silver.PayBill_WorkWeeks)
BEGIN
    INSERT INTO Silver.PayBill_WorkWeeks
    (PayrollWorkWeeksDatesID,WorkWeekYear,WorkWeekNumber,
     WorkWeekFromDate,WorkWeekToDate,PayrollWorkWeekFromDate,
     PayrollWorkWeekToDate,PayrollProcessingDate,IsBiWeeklyRegular)
    SELECT pww.Id,pww.Year,pww.WorkWeekNumber,
           CAST(pww.FromDateTime AS DATE),
           CAST(pww.ToDateTime AS DATE),
           CAST(pww.FromDateTime AS DATE),
           CAST(pww.ToDateTime AS DATE),
           DATEADD(DAY,1,CAST(pww.ToDateTime AS DATE)),
           pww.IsBiWeeklyRegular
    FROM nurses.Timecard.PayrollWorkWeekDates pww
    WHERE NOT EXISTS (SELECT 1 FROM Silver.PayBill_WorkWeeks t WHERE t.PayrollWorkWeeksDatesID=pww.Id);
END;
GO

/* ==========================================================
   8. Silver.PayBill_Timecards
   ========================================================== */
IF OBJECT_ID(N'Silver.PayBill_Timecards','U') IS NULL
BEGIN
    CREATE TABLE Silver.PayBill_Timecards(
        TimecardID INT,
        BrandID INT,
        FacilityID INT,
        ClinicianID INT,
        WorkWeekStartDate DATE,
        WorkWeekEndDate DATE,
        PayrollWorkWeekDatesID INT,
        ContractID INT,
        PayBillStatusID INT,
        PayBillRecordTypeID TINYINT,
        MigrationFlag BIT,
        IsReportable BIT,
        ETL_CreatedDate DATETIME DEFAULT GETDATE(),
        ETL_ModifiedDate DATETIME DEFAULT GETDATE()
    );
END;
GO

/* ==========================================================
   9. Silver.PayBill_PayrollDetails
   ========================================================== */
IF OBJECT_ID(N'Silver.PayBill_PayrollDetails','U') IS NULL
BEGIN
    CREATE TABLE Silver.PayBill_PayrollDetails(
        PayBillTypeID TINYINT DEFAULT 0,
        PayrollDetailsID INT NOT NULL PRIMARY KEY,
        TimeCardID INT,
        PayBillLineTypeID INT,
        UnitID INT,
        ExcludeFromPayBill BIT,
        AdjustedTimeCardID INT,
        PayBillRecordTypeID TINYINT,
        IsContraRevenue BIT,
        MigrationFlag BIT,
        IsEarlyPay BIT,
        ShiftDate DATE,
		BronzeCreatedDate DATE,
        ShiftDateStartTime TIME(0),
        ShiftDateEndTime TIME(0),
        Quantity DECIMAL(18,2),
        Lunch DECIMAL(18,2),
        Rate DECIMAL(18,2),
        Amount DECIMAL(18,2),
        PayBillStatusID TINYINT,
        ProcessedDate DATETIME,
        ProcessedByUserID NVARCHAR(450),
        PayBillCommentID INT,
        IsDeleted BIT,
        IsReportable BIT,
        TestFlag BIT,
        ETL_CreatedDate DATETIME2(0) DEFAULT GETDATE(),
        ETL_ModifiedDate DATETIME2(0) DEFAULT GETDATE()
    );
CREATE INDEX IX_PayBill_PayrollDetails_SourceKeys 
ON Silver.PayBill_PayrollDetails (TimeCardID, PayBillRecordTypeID, ProcessedDate);
--CREATE INDEX IX_PayBill_PayrollDetails_BronzeCreatedDate
--ON Silver.PayBill_PayrollDetails (BronzeCreatedDate)
--INCLUDE (PayrollDetailsID, ProcessedDate);
END;
GO

/* ==========================================================
   10. Silver.PayBill_OtherPayments_Pay
   ========================================================== */
IF OBJECT_ID(N'Silver.PayBill_OtherPayments_Pay','U') IS NULL
BEGIN
    CREATE TABLE Silver.PayBill_OtherPayments_Pay(
        PayBillTypeID TINYINT DEFAULT 1,
        OtherPaymentID INT NOT NULL PRIMARY KEY,
        TimeCardID INT,
        PayBillLineTypeID TINYINT,
        UnitID INT,
        BillHold BIT,
        AdjustedTimeCardID INT,
        PayBillRecordTypeID TINYINT,
        IsContraRevenue BIT,
        IsEarlyPay BIT,
        MigrationFlag BIT,
        ShiftDate DATE,
		BronzeCreatedDate DATE,
        Amount DECIMAL(18,2),
        PayBillStatusID TINYINT,
        ProcessedDate DATETIME,
        ProcessedByUserID NVARCHAR(450),
        PayBillCommentID INT,
        IsDeleted BIT,
        IsReportable BIT,
        TestFlag BIT,
        ETL_CreatedDate DATETIME2(0) DEFAULT GETDATE(),
        ETL_ModifiedDate DATETIME2(0) DEFAULT GETDATE()
    );
CREATE INDEX IX_PayBill_OtherPaymentsPay_SourceKeys  
ON Silver.PayBill_OtherPayments_Pay (OtherPaymentID, PayBillRecordTypeID, ProcessedDate);
END;
GO

/* ==========================================================
   11. Silver.PayBill_OtherPayments_Bill
   ========================================================== */
IF OBJECT_ID(N'Silver.PayBill_OtherPayments_Bill','U') IS NULL
BEGIN
    CREATE TABLE Silver.PayBill_OtherPayments_Bill(
        PayBillTypeID TINYINT DEFAULT 2,
        OtherPaymentID INT NOT NULL PRIMARY KEY,
        TimeCardID INT,
        PayBillLineTypeID TINYINT,
        UnitID INT,
        BillHold BIT,
        AdjustedTimeCardID INT,
        PayBillRecordTypeID TINYINT,
        IsContraRevenue BIT,
        IsEarlyPay BIT,
        MigrationFlag BIT,
        ShiftDate DATE,
		BronzeCreatedDate DATE,
        Amount DECIMAL(18,2),
        PayBillStatusID TINYINT,
        ProcessedDate DATETIME,
        ProcessedByUserID NVARCHAR(450),
        PayBillCommentID INT,
        IsDeleted BIT,
        IsReportable BIT,
        TestFlag BIT,
        ETL_CreatedDate DATETIME2(0) DEFAULT GETDATE(),
        ETL_ModifiedDate DATETIME2(0) DEFAULT GETDATE()
    );
CREATE INDEX IX_PayBill_OtherPaymentsBill_SourceKeys  
ON Silver.PayBill_OtherPayments_Bill (OtherPaymentID, PayBillRecordTypeID, ProcessedDate);
END;
GO

/* ==========================================================
   12. Silver.PayBill_Comments
   ========================================================== */
IF OBJECT_ID(N'Silver.PayBill_Comments','U') IS NULL
BEGIN
    CREATE TABLE Silver.PayBill_Comments(
        PayBillCommentID INT IDENTITY(1,1) PRIMARY KEY,
        Comments VARCHAR(888),
        Hashed_Comments BINARY(32) UNIQUE
    );
END;
IF NOT EXISTS (SELECT 1 FROM Silver.PayBill_Comments)
BEGIN
    INSERT INTO Silver.PayBill_Comments(Comments,Hashed_Comments)
    SELECT DISTINCT c, HASHBYTES('SHA2_256',c)
    FROM (
        SELECT Comments AS c FROM nurses.billing.PayrollDetails WHERE Comments IS NOT NULL AND Comments<>''
        UNION ALL
        SELECT Comments FROM nurses.billing.OtherPayments WHERE Comments IS NOT NULL AND Comments<>''
    ) q
    WHERE NOT EXISTS(SELECT 1 FROM Silver.PayBill_Comments t WHERE t.Hashed_Comments = HASHBYTES('SHA2_256',q.c));
END;
GO

/* ==========================================================
   13. Silver.Facilities — Add Column [IsNovaPay]
   ========================================================== */
IF NOT EXISTS (
    SELECT 1
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'Silver'
      AND TABLE_NAME = 'Facilities'
      AND COLUMN_NAME = 'IsNovaPay'
)
BEGIN
    ALTER TABLE Silver.Facilities
    ADD [IsNovaPay] BIT NULL;
END;

/* ==========================================================
   14. Silver.PayBill_PayDetails — Add Column [IsNovaPay]
   ========================================================== */
IF OBJECT_ID('Reporting.Silver.PayBill_PayDetails','U') IS NULL
BEGIN
  CREATE TABLE Reporting.Silver.PayBill_PayDetails (
      PayBillID        INT           NOT NULL PRIMARY KEY,   -- one row per PayBill
      PullWeekID       INT           NULL,                   -- maps to PayrollWorkWeeksDatesID
      ETL_CreatedDate  DATETIME2(0)  NOT NULL CONSTRAINT DF_PBPD_Created  DEFAULT (GETDATE()),
      ETL_ModifiedDate DATETIME2(0)  NOT NULL CONSTRAINT DF_PBPD_Modified DEFAULT (GETDATE())
  );
CREATE INDEX IX_PayBill_PayDetails_PullWeekID
  ON Reporting.Silver.PayBill_PayDetails (PullWeekID)
  INCLUDE (PayBillID);
END
GO

GO
