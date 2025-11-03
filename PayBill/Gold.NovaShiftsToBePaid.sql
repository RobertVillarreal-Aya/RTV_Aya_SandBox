USE [reporting];
GO

CREATE OR ALTER VIEW [Gold].[NovaShiftsToBePaid] 
AS
--select count(*) from [Gold].[NovaShiftsToBePaid] --28,824,464

--SELECT [PayBillTypeID], COUNT(*) FROM [Silver].[PayBill_Details] GROUP BY [PayBillTypeID]
/*
PayBillTypeID	counts
2				505,828
1				8,934,744
0				44,599,058
*/
--PayBillTypeID	Counts


--select count(*) from [nurses].[Billing].[PayrollDetails]

SELECT 
	b.[Brand] AS TimeCardBrand,
	v.[OperatingName] AS VendorName,
	cond.[StartDate] AS ContractStartDate,
	cond.[EndDate] AS ContractEndDate,
	cl.[Name] AS Caregiver,
	f.[FacilityName] AS TimeCardFacility,
	u.[UnitName] AS Unit,
	conps.[Profession] AS ContractProfession,
	conps.[Specialty] AS ContractSpecialty,
	IIF(p.[AdjustedTimeCardID] IS NULL, 'Yes', 'No') AS [IsAdjustment],
    P.[ShiftDate] AS ShiftDates,
    P.[ShiftDateStartTime] AS StartTime,
    P.[ShiftDateEndTime] AS EndTime,
	lt.[PayBillLineTypeDescription] AS LineType,
	P.[Lunch],
	P.[Quantity],
	P.[Rate],
    P.[Amount],
	comm.[Comments],
	ee.[Employee] AS PayrollRep,
	cone.[Recruiter],
	cl.[PayrollID],
	pw.PullWeekID AS PullWeekID,
	t.[ClinicianID],
	t.[FacilityID],
	f.[State] AS FacilityState,
	t.[ContractID],
	lob.[LOB] AS FacilityLineOfBusiness,
	t.[TimecardID],
	conps.[LineOfBusiness] AS  ContractLineOfBusiness,
	ps.[StatusName] AS PayBillStatus,
	ts.[StatusName] AS TimeCardStatus,
	f.[hospitalSystemId] AS HospitalSystemID,
    P.[IsEarlyPay],
	pbt.[PayBillType],
	f.[IsNovaPay],
	p.[MigrationFlag],
	cl.[APNorAya],
	p.[ProcessedDate] AS [ToBeProcessedDate], -- value same as field below
    P.[ProcessedDate],
    p.[PayBillTypeID],
	trt.[PayBillRecordType] AS [TimecardsPayBillRecordType],
	prt.[PayBillRecordType],
    p.[IsContraRevenue],
	p.[IsPay],
	p.[IsBill],
	trt.[IsIntercompany]
FROM [Silver].[PayBill_Details] P WITH (NOLOCK)
--PayBill
LEFT JOIN [Silver].[PayBill_Statuses] ps WITH (NOLOCK) ON ps.[PayBillStatusID] = p.[PayBillStatusID]
LEFT JOIN [Silver].[Units] u WITH (NOLOCK) ON u.[UnitID] = p.[UnitID]
LEFT JOIN [Silver].[PayBill_Types] pbt WITH (NOLOCK) ON pbt.[PayBillTypeID] = p.[PayBillTypeID]
LEFT JOIN [Silver].[PayBill_LineItemTypes] lt WITH (NOLOCK) ON lt.PayBillLineTypeID = p.PayBillLineTypeID
LEFT JOIN [Silver].[PayBill_Comments] comm WITH (NOLOCK) ON comm.[PayBillCommentID] = p.[PayBillCommentID]
LEFT JOIN [Silver].[PayBill_RecordTypes] prt WITH (NOLOCK) ON prt.[PayBillRecordTypeID]= p.[PayBillRecordTypeID] AND prt.PayBillRecordTypeClass = 'TransactionType'
LEFT JOIN Reporting.Silver.PayBill_PayDetails pw WITH (NOLOCK) ON pw.PayBillID = p.PayBillID
--TimeCards
LEFT JOIN [Silver].[PayBill_Timecards] t WITH (NOLOCK) ON t.TimecardID = p.[TimeCardID]
LEFT JOIN [Silver].[PayBill_RecordTypes] trt WITH (NOLOCK) ON trt.[PayBillRecordTypeID]= t.[PayBillRecordTypeID] AND trt.PayBillRecordTypeClass = 'TimecardType'
LEFT JOIN [Silver].[PayBill_Statuses] ts WITH (NOLOCK) ON ts.[PayBillStatusID] = t.[PayBillStatusID]
LEFT JOIN [Global].[Brands] b WITH (NOLOCK) ON b.[BrandID] = t.[BrandID]
LEFT JOIN [Silver].[Contracts] con WITH (NOLOCK) ON con.[ContractID] = t.[ContractID]
LEFT JOIN [Silver].[Contracts_ContractDates] cond WITH (NOLOCK) ON cond.[ContractID] = con.[ContractID]
LEFT JOIN [Silver].[Contracts_ProfessionSpecialties] conps WITH (NOLOCK) ON conps.[ContractID] = con.[ContractID]
LEFT JOIN [Silver].[Contracts_Employees] cone WITH (NOLOCK) ON cone.[ContractID] = con.[ContractID]
LEFT JOIN [Silver].[Clinicians] cl WITH (NOLOCK) ON cl.[ClinicianID] = t.ClinicianID
LEFT JOIN [Silver].[Clinicians_Vendors] clv WITH (NOLOCK) ON clv.[ClinicianID] = cl.ClinicianID 
LEFT JOIN [Silver].[Vendors] v WITH (NOLOCK) ON v.[VendorID] = clv.[VendorId]
LEFT JOIN [Silver].[Facilities] f WITH (NOLOCK) ON f.[FacilityID] = t.[FacilityID]
LEFT JOIN [Silver].[Employees] ee WITH (NOLOCK) ON ee.EmployeeID = f.payrollliaisonid
LEFT JOIN [dbo].[LineOfBusiness] lob WITH (NOLOCK) ON lob.Abbreviation = f.[Type]
WHERE 0 = 0
	--AND [ProcessedDate] > DATEADD(DAY, -7, GETDATE())
	AND p.[IsPay] = 1
	AND trt.[IsPay] = 1
	AND p.[IsReportable] = 1
	AND p.[TestFlag] = 0

