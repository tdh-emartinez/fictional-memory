defmodule Extract.Vision do
	def pull_mkt_site_users(instance_name) do
		batch_params = %{
			sobject: "Marketing_Site_Users_Groups__c",
			soql: "select contact__r.firstname, contact__r.lastname, contact__r.email, contact__r.marketing_site_username__c, Contact__r.Marketing_Site_Action__c, Group__r.Group_Number__c, Group__r.Profile_Name__c from marketing_site_users_groups__c where contact__r.marketing_site_username__c != null and contact__r.Marketing_Site_User__c = true and group__r.status__c = 'ACTIVE' and group__r.group_number__c != null and Contact__r.Contact_Status__c = 'Active' and Group__r.Account__r.Type != 'Former Customer' and allow_literature__c = true and odd_duck_msu__c = false",
			zip_annotation: "_std_msug"
		}
		SfdcApi.BatchV1Functions.create_batch_job(
			instance_name,
			batch_params)
	end

	def pull_odd_duck_msu(instance_name) do
		batch_params = %{
			sobject: "Marketing_Site_Users_Groups__c",
			soql: "select contact__r.firstname, contact__r.lastname, contact__r.email, contact__r.marketing_site_username__c, Contact__r.Marketing_Site_Action__c, Group__r.Group_Number__c, Group__r.Profile_Name__c from marketing_site_users_groups__c where contact__r.marketing_site_username__c != null and contact__r.Marketing_Site_User__c = true and group__r.status__c = 'ACTIVE' and group__r.group_number__c != null and Contact__r.Contact_Status__c = 'Active' and Group__r.Account__r.Type != 'Former Customer' and allow_literature__c = true and odd_duck_msu__c = true",
			zip_annotation: "_odd_ducks"
		}
		SfdcApi.BatchV1Functions.create_batch_job(
			instance_name,
			batch_params
		)
	end

	def pull_mkt_site_users_hub(instance_name) do
		batch_params = %{
			sobject: "Mkt_Site_User__c",
			soql: "select contact__r.firstname, contact__r.lastname, contact__r.email, contact__r.marketing_site_username__c, Contact__r.Marketing_Site_Action__c, Mbr_Group__r.Group_Number__c, Mbr_Group__r.Profile_Name__c from Mkt_Site_User__c  where contact__r.marketing_site_username__c != null and contact__r.Marketing_Site_User__c = true and Mbr_Group__r.status__c = 'ACTIVE' and Mbr_Group__r.group_number__c != null and Contact__r.Contact_Status__c = 'Active' and Mbr_Group__r.Client_Account__r.Type != 'Former Customer'",
			zip_annotation: "_msu_hub"
		}
		SfdcApi.BatchV1Functions.create_batch_job(
			instance_name,
			batch_params
		)

	end
	def pull_mbr_group_hub(instance_name) do
		batch_params = %{
			sobject: "Mbr_Group__c",
			soql: "select id, Group_Number__c, Profile_Name__c, Client_Account__c from Mbr_Group__c where is_valid_msu_group__c = true  order by Client_Account__c",
			zip_annotation: "_msu_mbr_groups"
		}
		SfdcApi.BatchV1Functions.create_batch_job(
			instance_name,
			batch_params
		)
	end
	def pull_mbr_group_role_hub(instance_name) do
		batch_params = %{
			sobject: "Mbr_Group_Role__c",
			soql: "select id,source_account__c, mbr_group__r.client_account__c, mbr_group__c from mbr_group_role__c where mbr_group__r.is_valid_msu_group__c = true and role_type__c in ('Payer','Benefit Sponsor') order by mbr_group__c",
			zip_annotation: "_msu_mbr_group_roles_hub"
		}
		SfdcApi.BatchV1Functions.create_batch_job(
			instance_name,
			batch_params
		)
	end
	def pull_mbr_group(instance_name) do
		batch_params = %{
			sobject: "Group__c",
			soql: "select id, Group_Number__c, Profile_Name__c, Account__c, Employer_Plan_Sponsor_for_Group__c, Employer_Plan_Sponsor_for_Group__r.Type, ASO_TPA__c, ASO_TPA__r.Type from Group__c where is_valid_msu_group__c = true  order by id",
			zip_annotation: "_msu_mbr_groups"
		}
		SfdcApi.BatchV1Functions.create_batch_job(
			instance_name,
			batch_params
		)
	end

	def pull_contacts(instance_name) do
		batch_params = %{
			sobject: "Contact",
			soql: "select id, accountid, firstname, lastname, email, marketing_site_username__c, marketing_site_action__c from contact where marketing_site_username__c != null and marketing_site_user__c = true and contact_status__c = 'Active' and Account.Type != 'Former Customer' order by AccountId",
			zip_annotation: "_msu_contacts"
		}
		SfdcApi.BatchV1Functions.create_batch_job(
			instance_name,
			batch_params
		)
	end
end
