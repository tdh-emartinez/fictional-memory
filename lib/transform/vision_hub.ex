defmodule Transform.VisionHub do
  def ingest_mbr_roles(instance_name) do
    IO.puts "loading group roles"
    raw_data = "#{Application.get_env(:sfdc_api, :base_dir)}/#{
      instance_name
    }/acecsv/Mbr_Group_Role__c_msu_mbr_group_roles_hub.csv.zip"
               |> Path.expand(__DIR__)
               |> File.open!([:read, :compressed])
               |> IO.binstream(:line)
      #		|> File.stream!
               |> CSV.decode!(strip_fields: true, headers: true)
               |> Enum.reduce(
                    [],
                    fn (row, tmp_array) ->
                      [
                        row | tmp_array
                      ]
                    end
                  )

    account_ids = MapSet.to_list(
      Enum.reduce(
        raw_data,
        MapSet.new(),
        fn row, acc -> MapSet.put(acc, row["Mbr_Group__c"]) end
      )
    )

    mapped_accounts = Enum.reduce(
      account_ids,
      %{},
      fn (account_id, acc) ->
        Map.put_new(acc, account_id, [])
      end
    )

    #IO.puts "mapped_accounts:"
    #IO.inspect mapped_accounts
    Enum.reduce(
      raw_data,
      mapped_accounts,
      fn row, acc ->
        tmp_array = get_in(acc, [row["Mbr_Group__c"]])
        #IO.puts "tmp_array:"
        #IO.inspect tmp_array
        put_in(acc, [row["Mbr_Group__c"]], tmp_array ++ [row["Source_Account__c"]])
      end
    )
  end
  def process_groups(instance_name, file_name) do
    account_contacts = ingest_contacts(instance_name)
    mbr_grp_role_accts = ingest_mbr_roles(instance_name)

    {:ok, file_pid} = file_name
                      |> Path.expand(__DIR__)
                      |> File.open([:write, :utf8])

    IO.write(
      file_pid,
      "Contact: First Name,Contact: Last Name,Contact: Email,Contact: Marketing Site  - Username,Contact: Marketing Site - User Group,Group: Group Number,Group: Profile Name\n"
    )

    IO.puts "processing groups"
    "#{Application.get_env(:sfdc_api, :base_dir)}/#{
      instance_name
    }/acecsv/Mbr_Group__c_msu_mbr_groups.csv.zip"
    |> Path.expand(__DIR__)
    |> File.open!([:read, :compressed])
    |> IO.binstream(:line)
      #		|> File.stream!
    |> CSV.decode!(strip_fields: true, headers: true)
    |> Enum.each(
         fn row ->
           tmp_accts = unless Map.has_key?(mbr_grp_role_accts, row["Id"]) do
             #IO.puts ""
             [row["Client_Account__c"]]
           else
             tmp_unvetted_accts = [row["Client_Account__c"]] ++ mbr_grp_role_accts[row["Id"]]
             MapSet.to_list(MapSet.new(tmp_unvetted_accts))
           end
           msu_batch = Map.put(row, "msu_accounts", tmp_accts)
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
    IO.puts "processing users"
    source_filename = "#{Application.get_env(:sfdc_api, :base_dir)}/#{
      instance_name
    }/acecsv/Mkt_Site_User__c_msu_hub.csv.zip"
                      |> Path.expand(__DIR__)
    transcribe_msu_overrides(source_filename, file_pid)
    File.close(file_pid)
  end
  defp transcribe_msu_overrides(source_filename, file_pid) do
    fixed_data = source_filename
                 |> File.open!([:read, :compressed])
                 |> IO.binstream(:line)
    #|> File.stream!
    #|> Stream.drop(1)
    msu_data = fixed_data
               |> CSV.decode!(
                    strip_fields: true,
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

    Enum.drop(msu_data, -1)
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
    |> Stream.drop(1)
    |> Enum.each(&IO.write(file_pid, &1))

  end
  defp generate_msu(group_info, account_contacts) do

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
    #IO.puts "Group: #{group_info["Group_Number__c"]}, Accounts: #{Enum.count(group_info["msu_accounts"])} , Contacts: #{Enum.count(tmp_contacts)}"
    #IO.inspect group_info["msu_accounts"]
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

  end
  def ingest_contacts(instance_name) do
    IO.puts "loading contacts"
    raw_data = "#{Application.get_env(:sfdc_api, :base_dir)}/#{instance_name}/acecsv/Contact_msu_contacts.csv.zip"
               |> Path.expand(__DIR__)
               |> File.open!([:read, :compressed])
               |> IO.binstream(:line)
      #		|> File.stream!
               |> CSV.decode!(strip_fields: true, headers: true)
               |> Enum.reduce(
                    [],
                    fn (row, tmp_array) ->
                      [
                        row | tmp_array
                      ]
                    end
                  )

    account_ids = Enum.reduce(
      raw_data,
      MapSet.new(),
      fn row, acc -> MapSet.put(acc, row["AccountId"]) end
    )

    mapped_accounts = Enum.reduce(
      account_ids,
      %{},
      fn (account_id, acc) ->
        Map.put_new(acc, account_id, [])
      end
    )
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
  def test_msu_override do
    instance_name = "hub_prod"
    {:ok, file_pid} = "#{Application.get_env(:sfdc_api, :base_dir)}/#{
      instance_name
    }/acecsv/msu_overrides.csv"
                      |> Path.expand(__DIR__)
                      |> File.open([:write, :utf8])

    source_filename = "#{Application.get_env(:sfdc_api, :base_dir)}/#{
      instance_name
    }/acecsv/Mkt_Site_User__c_msu_hub.csv.zip"
                      |> Path.expand(__DIR__)
    transcribe_msu_overrides(source_filename, file_pid)
    File.close(file_pid)
  end
end
