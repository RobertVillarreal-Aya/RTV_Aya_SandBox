USE [reporting]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROC [Silver].[UpdatePayBill_Timecards]
--(@EarliestDate DATE)
AS BEGIN
/****************************************************************************
EXEC [Silver].[UpdatePayBill_Timecards]
--exec reporting.[Silver].[UpdatePayBill_Timecards] @EarliestDate = '8-1-2025'
--exec reporting.[Silver].[UpdatePayBill_Timecards] @EarliestDate = NULL
--TRUNCATE TABLE reporting.Silver.PayBill_Timecards

 
Date        Author        Performance (rows/time)																	Ticket - notes
----------  ---------     ------------------------------------------------------									---------------------------
10/16/2025	Robert V	 Initial: Rows 9281097 | Time: 0m 32s   ||   Incremental: Rows Added 0 | Time: 0m 12s		TicketNum - Sproc Creation

***************************************************************************************************/

DROP TABLE IF EXISTS #temp_TimecardChanges
SELECT
	tc.id AS TimecardID
	, tc.BrandId
    , tc.FacilityId
	, tc.NurseId AS ClinicianID
	, ww.FromDateTime as WorkWeekStartDate
	, ww.toDateTime as WorkWeekEndDate
    , pww.id as PayrollWorkWeekDatesId
	, tc.ContractId
	, [ps].[PayBillStatusID] as PayBillStatusID
	,CASE 
		WHEN UserInfo.CompanyID= 1 and tc.[BrandId] <> Facility.BrandID then 4 -- AyaPayroll 
		WHEN UserInfo.CompanyID = 1 and tc.[BrandId] <> 1 Then 5 -- Lotus Billing 
		ELSE 6 -- Non-Intercompany 
	END PayBillRecordTypeID
	, IIF(tc.CreatedDate = '2024-08-30 09:12' AND c.FacilityID = '81748', 1, 0)  as MigrationFlag
	, 1 as IsReportable
INTO #temp_TimecardChanges
FROM
	nurses.TimeCard.TimeCards tc WITH (NOLOCK)
		LEFT JOIN nurses.dbo.UserInfo UserInfo WITH (NOLOCK) ON UserInfo.UserID = tc.NurseId
		LEFT JOIN nurses.Facility.FacilityBrands Facility WITH (NOLOCK) ON Facility.FacilityId = tc.FacilityId
		LEFT JOIN reporting.silver.Contracts c WITH (NOLOCK) ON tc.ContractId = c.ContractID
		LEFT JOIN nurses.TimeCard.workweekdates ww WITH (NOLOCK) ON tc.WorkWeekDatesId = ww.Id
		LEFT JOIN nurses.TimeCard.PayrollWorkWeekDates pww with (NOLOCK) on ww.ToDateTime <= pww.todatetime AND ww.todatetime >= pww.fromdatetime AND pww.Year = YEAR(ww.FromDateTime)
		LEFT JOIN [Silver].[PayBill_Statuses] ps WITH (NOLOCK) ON ps.PayBillStatusTypeID = 1 AND ps.PayBillSourceStatusID = tc.[StatusId]
WHERE
	0 = 0
--	AND (greatest(tc.UpdatedDate, tc.CreatedDate) >= @EarliestDate or @EarliestDate IS NULL)

/***********************************Final Insert/Update******************************************/

MERGE reporting.Silver.PayBill_Timecards AS t
	USING #temp_TimecardChanges AS s ON t.TimecardID = s.timecardid

	WHEN MATCHED AND (
		ISNULL(t.BrandID,99) <> ISNULL(s.BrandId,99)
		OR ISNULL(t.FacilityID,99) <> ISNULL(s.FacilityId,99)
		OR ISNULL(t.ClinicianID,99) <> ISNULL(s.ClinicianID,99)
		OR ISNULL(t.ContractID,99) <> ISNULL(s.ContractId,99)
		OR ISNULL(t.PayBillStatusID,99) <> ISNULL(s.PayBillStatusID,99)
		OR ISNULL(t.PayBillRecordTypeID,99) <> ISNULL(s.PayBillRecordTypeID,99)
		OR ISNULL(t.MigrationFlag, 0) <> ISNULL(s.MigrationFlag, 0)
		OR ISNULL(t.IsReportable, 0) <> ISNULL(s.IsReportable, 0)
		OR isnull(t.PayrollWorkWeekDatesID,99) <> isnull(s.PayrollWorkWeekDatesId,99)
		OR isnull(t.WorkWeekStartDate,'9999-1-1') <> isnull(s.WorkWeekStartDate,'9999-1-1')
		OR isnull(t.WorkWeekEndDate,'9999-1-1') <> isnull(s.WorkWeekEndDate,'9999-1-1')
		)
	THEN UPDATE SET
		t.BrandID = s.BrandId
		,t.FacilityID = s.FacilityId
		,t.ClinicianID = s.ClinicianID
		,t.ContractID = s.ContractId
		,t.PayBillStatusID = s.PayBillStatusID
		,t.PayBillRecordTypeID = s.PayBillRecordTypeID
		,t.MigrationFlag = s.MigrationFlag
		,t.IsReportable = s.IsReportable
		,t.PayrollWorkWeekDatesID = s.PayrollWorkWeekDatesId
		,t.WorkWeekStartDate = s.WorkWeekStartDate
		,t.WorkWeekEndDate = s.WorkWeekEndDate
		,t.ETL_ModifiedDate = GETDATE()

	WHEN NOT MATCHED BY TARGET
		THEN
        INSERT (timecardid, brandid, facilityid, ClinicianID, workweekstartdate, workweekenddate, payrollworkweekdatesid, contractid, PayBillStatusID, PayBillRecordTypeID, migrationflag, isreportable, etl_createddate, etl_modifieddate)
			VALUES (s.TimecardID, s.BrandId, s.FacilityId, s.ClinicianID, s.WorkWeekStartDate, s.WorkWeekEndDate, s.PayrollWorkWeekDatesId, s.ContractId, s.PayBillStatusID, s.PayBillRecordTypeID, s.MigrationFlag,  s.IsReportable, GETDATE(), GETDATE());

END