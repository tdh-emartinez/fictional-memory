defmodule Transform.Vision do
	def process_groups(instance_name, file_name) do
		account_contacts = ingest_contacts(instance_name)

		{:ok, file_pid} = file_name
											|> Path.expand(__DIR__)
											|> File.open([:write, :utf8])

		IO.write(
			file_pid,
			"Contact: First Name,Contact: Last Name,Contact: Email,Contact: Marketing Site  - Username,Contact: Marketing Site - User Group,Group: Group Number,Group: Profile Name\n"
		)

		"#{Application.get_env(:sfdc_api, :base_dir)}/#{
			instance_name
		}/acecsv/Group__c_msu_mbr_groups.csv.zip"
		|> Path.expand(__DIR__)
		|> File.open!([:read, :compressed])
		|> IO.binstream(:line)
			#		|> File.stream!
		|> CSV.decode!(strip_fields: true, headers: true)
		|> Enum.each(
				 fn row ->
					 tmp_set = MapSet.new()
					 tmp_set = MapSet.put(tmp_set, row["Account__c"])
					 tmp_set = unless row["ASO_TPA__r.Type"] == "" || row["ASO_TPA__r.Type"] == "Former Customer" do
						 MapSet.put(tmp_set, row["ASO_TPA__c"])
					 else
						 tmp_set
					 end
					 tmp_set = unless row["Employer_Plan_Sponsor_for_Group__r.Type"] == "" || row["Employer_Plan_Sponsor_for_Group__r.Type"] == "Former Customer" do
						 MapSet.put(tmp_set, row["Employer_Plan_Sponsor_for_Group__c"])
					 else
						 tmp_set
					 end

					 msu_batch = Map.put(row, "msu_accounts", MapSet.to_list(tmp_set))
											 |> generate_msu(account_contacts)
											 |> CSV.encode(
														delimiter: "\n",
														headers: [
															"Contact: First Name",
															"Contact: Last Name",
															"Contact: Email",
															"Contact: Marketing Site  - Username",
															"Contact: Marketing Site - User Group",
															"Group: Group Number",
															"Group: Profile Name"
														]
													)
											 |> Enum.reduce([], fn row, acc -> [row | acc] end)

					 new_batch = Enum.drop(msu_batch, -1)

					 IO.write(file_pid, new_batch)

				 end
			 )
		"#{Application.get_env(:sfdc_api, :base_dir)}/#{
			instance_name
		}/acecsv/Marketing_Site_Users_Groups__c_odd_ducks.csv.zip"
		|> Path.expand(__DIR__)
		|> File.open!([:read, :compressed])
		|> IO.binstream(:line)
			#|> File.stream!
		|> Stream.drop(1)
		|> Stream.each(&(IO.write(file_pid, &1)))
		|> Stream.run()

		File.close(file_pid)
	end
	defp generate_msu(group_info, account_contacts) do
		#		IO.puts DateTime.to_string(DateTime.utc_now())
		tmp_contacts = Enum.reduce(
			group_info["msu_accounts"],
			[],
			fn account, acc ->
				if account_contacts[account] == nil do
					#IO.puts account
					acc
				else
					acc ++ account_contacts[account]
				end

			end
		)
		#		IO.puts DateTime.to_string(DateTime.utc_now())
		Stream.map(
			tmp_contacts,
			fn tmp_rec ->
				%{
					"Contact: First Name" => tmp_rec["FirstName"],
					"Contact: Last Name" => tmp_rec["LastName"],
					"Contact: Email" => tmp_rec["Email"],
					"Contact: Marketing Site  - Username" => tmp_rec["Marketing_Site_Username__c"],
					"Contact: Marketing Site - User Group" => tmp_rec["Marketing_Site_Action__c"],
					"Group: Group Number" => group_info["Group_Number__c"],
					"Group: Profile Name" => group_info["Profile_Name__c"]
				}
			end
		)
		#		IO.puts DateTime.to_string(DateTime.utc_now())
	end
	defp ingest_contacts(instance_name) do
		IO.puts DateTime.to_string(DateTime.utc_now())
		raw_data = "#{Application.get_env(:sfdc_api, :base_dir)}/#{instance_name}/acecsv/Contact_msu_contacts.csv.zip"
							 |> Path.expand(__DIR__)
							 |> File.open!([:read, :compressed])
							 |> IO.binstream(:line)
			#|> File.stream!
							 |> CSV.decode!(strip_fields: true, headers: true)
							 |> Enum.reduce(
										[],
										fn (row, tmp_array) ->
											[
												row | tmp_array
											]
										end
									)
		IO.puts Enum.count(raw_data)
		IO.puts DateTime.to_string(DateTime.utc_now())
		account_ids = Enum.reduce(
			raw_data,
			MapSet.new(),
			fn row, acc -> MapSet.put(acc, row["AccountId"]) end
		)
		IO.puts Enum.count(account_ids)
		IO.puts DateTime.to_string(DateTime.utc_now())
		mapped_accounts = Enum.reduce(
			account_ids,
			%{},
			fn (account_id, acc) ->
				Map.put_new(acc, account_id, [])
			end
		)
		IO.puts Map.size(mapped_accounts)
		IO.puts DateTime.to_string(DateTime.utc_now())
		Enum.reduce(
			raw_data,
			mapped_accounts,
			fn row, acc ->
				tmp_array = get_in(acc, [row["AccountId"]])
				#IO.inspect tmp_array
				put_in(acc, [row["AccountId"]], [row | tmp_array])
			end
		)
	end
end
