defmodule Sfdc.Etl.CalcGroupMetrics do
  @moduledoc false
  def calc_contact_metrics(instance_name, msu_filename) do
    IO.puts DateTime.to_string(DateTime.utc_now())
    mapped_usernames = "#{Application.get_env(:sfdc_api, :base_dir)}/#{
      instance_name
    }/acecsv/Contact_msu_contacts.csv.zip"
                       |> Path.expand(__DIR__)
                       |> File.open!([:read, :compressed])
                       |> IO.binstream(:line)
      #		|> File.stream!
                       |> CSV.decode!(strip_fields: true, headers: true)
                       |> Enum.reduce(
                            %{},
                            fn (row, tmp_map) ->
                              Map.put(tmp_map, row["Marketing_Site_Username__c"], row["Id"])
                            end
                          )

    IO.puts DateTime.to_string(DateTime.utc_now())
    mapped_metrics = msu_filename
                     |> File.stream!
                     |> CSV.decode!(strip_fields: true, headers: true)
                     |> Enum.reduce(
                          %{},
                          fn (row, tmp_map) ->
                            #username = String.downcase(row["Contact: Marketing Site  - Username"])
                            username = row["Contact: Marketing Site  - Username"]
                            if Map.has_key?(tmp_map, username) do
                              Map.put(tmp_map, username, tmp_map[username] + 1)
                            else
                              Map.put_new(tmp_map, username, 1)
                            end
                          end
                        )

    {:ok, file_pid} = "#{Application.get_env(:sfdc_api, :base_dir)}/#{
      instance_name
    }/acecsv/msu_metrics.csv"
                      |> Path.expand(__DIR__)
                      |> File.open([:write, :utf8])

    IO.write(
      file_pid,
      "Id,Group_Count__c\n"
    )

    Enum.each(
      Map.keys(mapped_usernames),
      fn username ->
        if Map.has_key?(mapped_metrics, username) do
          IO.write(file_pid, "#{mapped_usernames[username]},#{mapped_metrics[username]}\n")
          else
          IO.write(file_pid, "#{mapped_usernames[username]},0\n")
        end
      end
    )
    File.close(file_pid)
    IO.puts DateTime.to_string(DateTime.utc_now())
  end

end
