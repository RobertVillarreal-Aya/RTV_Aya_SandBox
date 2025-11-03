USE [reporting]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROC [Silver].[UpdateFacilities] AS BEGIN
/********************************************************************************** 
EXEC [Silver].[UpdateFacilities]
DATE				DEV					PERFORMANCE			TICKET - NOTES

----------Last three notes migrated from View----------
2023-10-23			Sumit P				25s, 73k rows		BIDEP-11636 - Added 'FacilityActive' column
2023-10-30			Robert V			28s, 73k rows		BIDEP-13871 - Added 'EarningPowerFacilities'
2024-03-04			Eric F				22s, 78k rows		BIDEP-18779 - Added 'Timekeeping' and 'Workweek'															   

----------New SPROC Notes----------
2024-05-18			Will W				9s, 79k 			HOT - Created SPROC off of PowerBI.Facilities View (last update 3/4/2024)
2024-06-12			Robert V			14s, 80k 			BIDEP-24682 - Added StateClassification field
2024-06-17			Joe M				4s, 80k 			Star Schema Silver Table
2024-08-07			Eric F				4s, 80k 			Adding fields to support new spoke [UpdateFacilities_Accounts]
2024-10-09			Robert V			4s, 80k 			BIDEP-33666 - Update Subcontract_Text to include Yes with Approval
2025-01-17          Armstrong A         4s, 80k				BIDEP-38823 - Added IncludeCredentialingServices
2025-04-28			Robert V			4s, 80k				BIDEP-42754 - Added AccountManagerID
2025-05-15			Austen M			6s, 87k 			BIDEP-44397 - Added ElectronicWorkOrderConfirmation
2025-07-02			Javier R			12s, 105k			Added TimekeepingScheduleID
2025-07-08			Cynthia W			12s, 111k 			Added RankID
2025-10-07			Erik S				3s, 3k 				DTR-9626 - Pulled ElectronicWorkOrderConfirmation logic out of view, made TypeFullName dynamic
***********************************************************************************/

MERGE [Silver].[Facilities] t
USING (
	SELECT
		p.Id 'FacilityID'
		,p.Name 'FacilityName'
		,CASE WHEN p.Name LIKE '%Credentialing Services%' THEN 1 ELSE 0 END 'IncludeCredentialingServicesID'
		,p.hospType 'Type'
		,p.hospAddress 'Address'
		,p.hospCity 'City'
		,LEFT(p.hospState, 2) 'State'
		,p.hospStateId 'StateID'
		,he.vmsProviderId 'MSPId'
		,p.ContractFacilitySystemId 'ContractGroupID'
		,he.hospitalSystemId
		,he.vmsSoftwareId
		,p.QMSystemCode 'QMAssociationID'
		,p.acctMgrID 'AMID'
		,p.paybackPct
		,LEFT(p.hospZip, 5) 'ZipCode'
		,he.subcontractor 'Subcontracting'
		,CASE
			 WHEN ISNULL(he.subcontractor, 0) = 0 THEN 'No'
			 WHEN he.subcontractor = 1 AND he.subcontractorReqApproval = 1 THEN 'Yes with Approval'
			 ELSE 'Yes'
		 END 'Subcontracting_Text'
		,p.payrollLiasonId 'PayrollLiaisonID'
		,p.lotusPayrollLiasonId 'LotusPayrollLiaisonID'
		,he.billHoldTimeSystem 'BillHoldTimeSystemID'
		,he.primaryTimeSystem 'PrimaryTimeSystemID'
		,he.payrollWorkWeek 'PayrollWorkWeekID'
		,CASE
			 WHEN p.callForJobs = 1 AND he.vmsProviderId = 1495 THEN 'Aya MSP'
			 WHEN p.callForJobs = 1 AND he.vmsProviderId = 233 THEN 'Healthcare Select MSP'
			 WHEN p.callForJobs = 1 AND he.vmsProviderId = 1629 THEN 'Qualivis MSP'
			 WHEN p.callForJobs = 1 AND he.vmsProviderId = 1565 THEN 'Symmetry MSP'
			 WHEN he.vmsProviderId = 186 THEN 'Qualivis PA'
			 WHEN p.callForJobs NOT IN (1, 6) AND he.vmsProviderId = 97 THEN 'Aya Direct Contract'
			 WHEN p.callForJobs = 6 THEN 'AyaPriority'
			 ELSE 'Other'
		 END 'AccountType'
		,CASE WHEN p.callForJobs = 6 THEN 'Yes' ELSE 'No' END 'IsPriority'
		,p.hospTotStaffedBeds 'Beds'
		,p.facilityProfileGroupId 'FacilityProfileGroupID'
		,p.hospTypeofcontrol 'TypeOfControl'
		,ISNULL(p.enteredTime, '2000-01-01') 'DateCreated'
		,p.hospPhone 'FacilityPhoneNumber'
		,he.CMSID
		,he.pspMgrId 'MSPManagerID'
		,he.clinicalMgrID 'ClinicalManagerID'
		,p.originatorID 'AccountOriginatorID'
		,p.AlliedAccountManagerID
		,p.acctMgrID 'AccountManagerID'
		,p.AccountSpecialistId
		,he.coordId 'AccountCoordinatorID'
		,he.CollectionRepId 'AccountsReceivableSpecialistID'
		,he.BillingSpecialist 'BillingSpecialistID'
		,CASE WHEN p.IsPR = 1 THEN 'Yes' ELSE 'No' END 'PlacementRestriction'
		,p.PRNum 'PlacementRestrictionValue'
		,CONVERT(VARCHAR(3000), p.PR_Reason) 'PlacementRestrictionNotes'
		,CASE
			 WHEN p.callForJobs = 1 THEN 1
			 WHEN p.callForJobs = 6 THEN 2
			 ELSE 3
		 END 'FacilityEmojiID'
		,NULLIF(CONVERT(VARCHAR(MAX), p.csystem), '') 'ChartingSoftware'
		,he.ProgramManagerId
		,p.OrientHours 'FacilityNBOHours'
		,CASE
			 WHEN p.OrientType = '$' THEN '$' + CONVERT(VARCHAR, p.OrientRate)
			 WHEN p.OrientType = '%' THEN CONVERT(VARCHAR, p.OrientRate) + '%'
			 ELSE CONVERT(VARCHAR, p.OrientRate)
		 END 'FacilityNBORate'
		,p.OrientHoursAlt 'FacilityNBOHoursAlt'
		,CASE
			 WHEN p.OrientTypeAlt = '$' THEN '$' + CONVERT(VARCHAR, p.OrientRateAlt)
			 WHEN p.OrientTypeAlt = '%' THEN CONVERT(VARCHAR, p.OrientRateAlt) + '%'
			 ELSE CONVERT(VARCHAR, p.OrientRateAlt)
		 END 'FacilityNBORateAlt'
		,CONVERT(VARCHAR(3000), p.OrientDetails) 'FacilityNBODetails'
		,CASE
			 WHEN p.facility_type IN (1, 23, 24) THEN 'Acute'
			 WHEN p.facility_type IN (4, 5, 8, 10, 15, 16, 17, 18, 20, 25, 29, 32) THEN 'Non-Acute'
			 ELSE 'Other'
		 END 'PDFacilityType'
		,CASE ht.HospTypeID
			WHEN 7 THEN 'Workforce Disruption'
			WHEN 8 THEN 'Workforce Disruption Non-Clinical'
			ELSE ht.HospTypeText
		 END 'TypeFullName'
		,CASE WHEN COALESCE(p.IsDNS, 0) = 0 THEN 'No' ELSE 'Yes' END CreditHoldFlag
		,CASE WHEN p.isTeaching = 1 THEN 'Yes' ELSE 'No' END 'Teaching'
		,p.ContractFacilitySystemId
		,CASE
			 WHEN tf.FacilityID IS NOT NULL OR ts.SystemID IS NOT NULL THEN 1
			 ELSE 0
		 END 'Test_Flag'
		,CASE
			 WHEN tf.FacilityID IS NOT NULL OR ts.SystemID IS NOT NULL THEN 0
			 ELSE 1
		 END 'Is_Reportable'
		,CASE WHEN ff.HospId IS NULL THEN 0 ELSE 1 END 'ElectronicWorkOrderConfirmation'
		,he.timeSched 'TimekeepingScheduleID'
		,p.callForJobs 'RankID'
		,he.[IsUsingNovaForPayroll] AS IsNovaPay 
	FROM nurses.dbo.FacilityProfiles p WITH (NOLOCK)
		LEFT JOIN nurses.dbo.infoHospitalExpanded he WITH (NOLOCK) ON he.hospid = p.Id
			LEFT JOIN reporting.dbo.TestSystems ts WITH (NOLOCK) ON ts.SystemID = he.hospitalSystemId
		LEFT JOIN (
			SELECT HospId
			FROM nurses.dbo.FacilityFeature WITH (NOLOCK)
			WHERE ApplicationFeatureId = 23 -- ElectronicWocSigning
		) ff ON ff.HospId = p.Id
		LEFT JOIN reporting.dbo.TestFacilities tf WITH (NOLOCK) ON tf.FacilityID = p.Id
		LEFT JOIN nurses.dbo.lookup_HospitalTypes ht WITH (NOLOCK) ON ht.HospType = p.hospType
) s ON t.[FacilityID] = s.[FacilityID]
WHEN MATCHED AND (
	ISNULL(t.[FacilityName],-1) <> ISNULL(s.[FacilityName],-1) OR
	ISNULL(t.[Type],-1) <> ISNULL(s.[Type],-1) OR
	ISNULL(t.[Address],-1) <> ISNULL(s.[Address],-1) OR
	ISNULL(t.[City],-1) <> ISNULL(s.[City],-1) OR
	ISNULL(t.[State],-1) <> ISNULL(s.[State],-1) OR
	ISNULL(t.[StateID],-1) <> ISNULL(s.[StateID],-1) OR
	ISNULL(t.[MSPId],-1) <> ISNULL(s.[MSPId],-1) OR
	ISNULL(t.[ContractGroupID],-1) <> ISNULL(s.[ContractGroupID],-1) OR
	ISNULL(t.[hospitalSystemId],-1) <> ISNULL(s.[hospitalSystemId],-1) OR
	ISNULL(t.[vmsSoftwareId],-1) <> ISNULL(s.[vmsSoftwareId],-1) OR
	ISNULL(t.[QMAssociationID],-1) <> ISNULL(s.[QMAssociationID],-1) OR
	ISNULL(t.[AMID],-1) <> ISNULL(s.[AMID],-1) OR
	ISNULL(t.[paybackPct],-1) <> ISNULL(s.[paybackPct],-1) OR
	ISNULL(t.[ZipCode],-1) <> ISNULL(s.[ZipCode],-1) OR
	ISNULL(t.[Subcontracting],-1) <> ISNULL(s.[Subcontracting],-1) OR
	ISNULL(t.[Subcontracting_Text],-1) <> ISNULL(s.[Subcontracting_Text],-1) OR
	ISNULL(t.[PayrollLiaisonID],-1) <> ISNULL(s.[PayrollLiaisonID],-1) OR
	ISNULL(t.[LotusPayrollLiaisonID],-1) <> ISNULL(s.[LotusPayrollLiaisonID],-1) OR
	ISNULL(t.[BillHoldTimeSystemID],-1) <> ISNULL(s.[BillHoldTimeSystemID],-1) OR
	ISNULL(t.[PrimaryTimeSystemID],-1) <> ISNULL(s.[PrimaryTimeSystemID],-1) OR
	ISNULL(t.[PayrollWorkWeekID],-1) <> ISNULL(s.[PayrollWorkWeekID],-1) OR
	ISNULL(t.[AccountType],-1) <> ISNULL(s.[AccountType],-1) OR
	ISNULL(t.[IsPriority],-1) <> ISNULL(s.[IsPriority],-1) OR
	ISNULL(t.[Beds],-1) <> ISNULL(s.[Beds],-1) OR
	ISNULL(t.[FacilityProfileGroupID],-1) <> ISNULL(s.[FacilityProfileGroupID],-1) OR
	ISNULL(t.[TypeOfControl],-1) <> ISNULL(s.[TypeOfControl],-1) OR
	ISNULL(t.[DateCreated],-1) <> ISNULL(s.[DateCreated],-1) OR
	ISNULL(t.[FacilityPhoneNumber],-1) <> ISNULL(s.[FacilityPhoneNumber],-1) OR
	ISNULL(t.[CMSID],-1) <> ISNULL(s.[CMSID],-1) OR
	ISNULL(t.[MSPManagerID],-1) <> ISNULL(s.[MSPManagerID],-1) OR
	ISNULL(t.[ClinicalManagerID],-1) <> ISNULL(s.[ClinicalManagerID],-1) OR
	ISNULL(t.[AccountOriginatorID],-1) <> ISNULL(s.[AccountOriginatorID],-1) OR
	ISNULL(t.[AlliedAccountManagerID],-1) <> ISNULL(s.[AlliedAccountManagerID],-1) OR
	ISNULL(t.[AccountSpecialistId],-1) <> ISNULL(s.[AccountSpecialistId],-1) OR
	ISNULL(t.[AccountCoordinatorID],-1) <> ISNULL(s.[AccountCoordinatorID],-1) OR
	ISNULL(t.[AccountsReceivableSpecialistID],-1) <> ISNULL(s.[AccountsReceivableSpecialistID],-1) OR
	ISNULL(t.[BillingSpecialistID],-1) <> ISNULL(s.[BillingSpecialistID],-1) OR
	ISNULL(t.[PlacementRestriction],-1) <> ISNULL(s.[PlacementRestriction],-1) OR
	ISNULL(t.[PlacementRestrictionValue],-1) <> ISNULL(s.[PlacementRestrictionValue],-1) OR
	ISNULL(t.[PlacementRestrictionNotes],-1) <> ISNULL(s.[PlacementRestrictionNotes],-1) OR
	ISNULL(t.[FacilityEmojiID],-1) <> ISNULL(s.[FacilityEmojiID],-1) OR
	ISNULL(t.[ChartingSoftware],-1) <> ISNULL(s.[ChartingSoftware],-1) OR
	ISNULL(t.[ProgramManagerID],-1) <> ISNULL(s.[ProgramManagerID],-1) OR
	ISNULL(t.[FacilityNBOHours],-1) <> ISNULL(s.[FacilityNBOHours],-1) OR
	ISNULL(t.[FacilityNBORate],-1) <> ISNULL(s.[FacilityNBORate],-1) OR
	ISNULL(t.[FacilityNBOHoursAlt],-1) <> ISNULL(s.[FacilityNBOHoursAlt],-1) OR
	ISNULL(t.[FacilityNBORateAlt],-1) <> ISNULL(s.[FacilityNBORateAlt],-1) OR
	ISNULL(t.[FacilityNBODetails],-1) <> ISNULL(s.[FacilityNBODetails],-1) OR
	ISNULL(t.[PDFacilityType],-1) <> ISNULL(s.[PDFacilityType],-1) OR
	ISNULL(t.[TypeFullName],-1) <> ISNULL(s.[TypeFullName],-1) OR
	ISNULL(t.[CreditHoldFlag],-1) <> ISNULL(s.[CreditHoldFlag],-1) OR
	ISNULL(t.[Teaching],-1) <> ISNULL(s.[Teaching],-1) OR
	ISNULL(t.[ContractFacilitySystemID],-1) <> ISNULL(s.[ContractFacilitySystemID],-1) OR
	ISNULL(t.[IncludeCredentialingServicesID],-1) <> ISNULL(s.[IncludeCredentialingServicesID],-1) OR
	ISNULL(t.[AccountManagerID],-1) <> ISNULL(s.[AccountManagerID],-1) OR
	ISNULL(t.[ElectronicWorkOrderConfirmation],-1) <> ISNULL(s.[ElectronicWorkOrderConfirmation],-1) OR
	ISNULL(t.[TimekeepingScheduleID],-1) <> ISNULL(s.[TimekeepingScheduleID],-1) OR
	ISNULL(t.[RankID],-1) <> ISNULL(s.[RankID],-1) OR
	ISNULL(t.[IsNovaPay],-1) <> ISNULL(s.[IsNovaPay],-1)
)
THEN UPDATE SET 
	t.[FacilityName] = s.[FacilityName],
	t.[Type] = s.[Type],
	t.[Address] = s.[Address],
	t.[City] = s.[City],
	t.[State] = s.[State],
	t.[StateID] = s.[StateID],
	t.[MSPId] = s.[MSPId],
	t.[ContractGroupID] = s.[ContractGroupID],
	t.[hospitalSystemId] = s.[hospitalSystemId],
	t.[vmsSoftwareId] = s.[vmsSoftwareId],
	t.[QMAssociationID] = s.[QMAssociationID],
	t.[AMID] = s.[AMID],
	t.[paybackPct] = s.[paybackPct],
	t.[ZipCode] = s.[ZipCode],
	t.[Subcontracting] = s.[Subcontracting],
	t.[Subcontracting_Text] = s.[Subcontracting_Text],
	t.[PayrollLiaisonID] = s.[PayrollLiaisonID],
	t.[LotusPayrollLiaisonID] = s.[LotusPayrollLiaisonID],
	t.[BillHoldTimeSystemID] = s.[BillHoldTimeSystemID],
	t.[PrimaryTimeSystemID] = s.[PrimaryTimeSystemID],
	t.[PayrollWorkWeekID] = s.[PayrollWorkWeekID],
	t.[AccountType] = s.[AccountType],
	t.[IsPriority] = s.[IsPriority],
	t.[Beds] = s.[Beds],
	t.[FacilityProfileGroupID] = s.[FacilityProfileGroupID],
	t.[TypeOfControl] = s.[TypeOfControl],
	t.[DateCreated] = s.[DateCreated],
	t.[FacilityPhoneNumber] = s.[FacilityPhoneNumber],
	t.[CMSID] = s.[CMSID],
	t.[MSPManagerID] = s.[MSPManagerID],
	t.[ClinicalManagerID] = s.[ClinicalManagerID],
	t.[AccountOriginatorID] = s.[AccountOriginatorID],
	t.[AlliedAccountManagerID] = s.[AlliedAccountManagerID],
	t.[AccountSpecialistId] = s.[AccountSpecialistId],
	t.[AccountCoordinatorID] = s.[AccountCoordinatorID],
	t.[AccountsReceivableSpecialistID] = s.[AccountsReceivableSpecialistID],
	t.[BillingSpecialistID] = s.[BillingSpecialistID],
	t.[PlacementRestriction] = s.[PlacementRestriction],
	t.[PlacementRestrictionValue] = s.[PlacementRestrictionValue],
	t.[PlacementRestrictionNotes] = s.[PlacementRestrictionNotes],
	t.[FacilityEmojiID] = s.[FacilityEmojiID],
	t.[ChartingSoftware] = s.[ChartingSoftware],
	t.[ProgramManagerID] = s.[ProgramManagerID],
	t.[FacilityNBOHours] = s.[FacilityNBOHours],
	t.[FacilityNBORate] = s.[FacilityNBORate],
	t.[FacilityNBOHoursAlt] = s.[FacilityNBOHoursAlt],
	t.[FacilityNBORateAlt] = s.[FacilityNBORateAlt],
	t.[FacilityNBODetails] = s.[FacilityNBODetails],
	t.[PDFacilityType] = s.[PDFacilityType],
	t.[TypeFullName] = s.[TypeFullName],
	t.[CreditHoldFlag] = s.[CreditHoldFlag],
	t.[Teaching] = s.[Teaching],
	t.[ContractFacilitySystemID] = s.[ContractFacilitySystemID],
	t.[IncludeCredentialingServicesID] = s.[IncludeCredentialingServicesID],
	t.[AccountManagerID] = s.[AccountManagerID],
	t.[ElectronicWorkOrderConfirmation] = s.[ElectronicWorkOrderConfirmation],
	t.[TimekeepingScheduleID] = s.[TimekeepingScheduleID],
	t.[RankID] = s.[RankID],
	t.[IsNovaPay] = s.[IsNovaPay],
	t.[ETL_ModifiedDate] = GETDATE()
WHEN NOT MATCHED BY TARGET
	THEN INSERT ([FacilityID],[FacilityName],[Type],[Address],[City],[State],[StateID],[MSPId],[ContractGroupID],[hospitalSystemId],[vmsSoftwareId],[QMAssociationID],[AMID],[paybackPct],[ZipCode],[Subcontracting],[Subcontracting_Text],[PayrollLiaisonID],[LotusPayrollLiaisonID],[BillHoldTimeSystemID],[PrimaryTimeSystemID],[PayrollWorkWeekID],[AccountType],[IsPriority],[Beds],[FacilityProfileGroupID],[TypeOfControl],[DateCreated],[FacilityPhoneNumber],[CMSID],[MSPManagerID],[ClinicalManagerID],[AccountOriginatorID],[AlliedAccountManagerID],[AccountSpecialistId],[AccountCoordinatorID],[AccountsReceivableSpecialistID],[BillingSpecialistID],[PlacementRestriction],[PlacementRestrictionValue],[PlacementRestrictionNotes],[FacilityEmojiID],[ChartingSoftware],[ProgramManagerID],[FacilityNBOHours],[FacilityNBORate],[FacilityNBOHoursAlt],[FacilityNBORateAlt],[FacilityNBODetails],[PDFacilityType],[TypeFullName],[CreditHoldFlag],[Teaching],[ContractFacilitySystemID],[IncludeCredentialingServicesID],[AccountManagerID],[ElectronicWorkOrderConfirmation],[TimekeepingScheduleID],[RankID],[IsNovaPay])
	VALUES (s.[FacilityID],s.[FacilityName],s.[Type],s.[Address],s.[City],s.[State],s.[StateID],s.[MSPId],s.[ContractGroupID],s.[hospitalSystemId],s.[vmsSoftwareId],s.[QMAssociationID],s.[AMID],s.[paybackPct],s.[ZipCode],s.[Subcontracting],s.[Subcontracting_Text],s.[PayrollLiaisonID],s.[LotusPayrollLiaisonID],s.[BillHoldTimeSystemID],s.[PrimaryTimeSystemID],s.[PayrollWorkWeekID],s.[AccountType],s.[IsPriority],s.[Beds],s.[FacilityProfileGroupID],s.[TypeOfControl],s.[DateCreated],s.[FacilityPhoneNumber],s.[CMSID],s.[MSPManagerID],s.[ClinicalManagerID],s.[AccountOriginatorID],s.[AlliedAccountManagerID],s.[AccountSpecialistId],s.[AccountCoordinatorID],s.[AccountsReceivableSpecialistID],s.[BillingSpecialistID],s.[PlacementRestriction],s.[PlacementRestrictionValue],s.[PlacementRestrictionNotes],s.[FacilityEmojiID],s.[ChartingSoftware],s.[ProgramManagerID],s.[FacilityNBOHours],s.[FacilityNBORate],s.[FacilityNBOHoursAlt],s.[FacilityNBORateAlt],s.[FacilityNBODetails],s.[PDFacilityType],s.[TypeFullName],s.[CreditHoldFlag],s.[Teaching],s.[ContractFacilitySystemID],s.[IncludeCredentialingServicesID],s.[AccountManagerID],s.[ElectronicWorkOrderConfirmation],s.[TimekeepingScheduleID],s.[RankID],s.[IsNovaPay])
WHEN NOT MATCHED BY SOURCE
	THEN DELETE;
END 
 
GO


