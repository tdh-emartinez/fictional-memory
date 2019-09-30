defmodule Mix.Tasks.ProcessVisionFileHub do
  use Mix.Task
  def run(_) do
    instance_name = "hub_prod"
    {:ok, _} = :application.ensure_all_started(:httpoison)
    {:ok, _} = :application.ensure_all_started(:sfdc_api)


    msu_file_date = Timex.Format.DateTime.Formatters.Strftime.format!(Timex.now(), "%Y_%0m_%0d")
    msu_file_name = "#{Application.get_env(:sfdc_api, :base_dir)}/#{instance_name}/vision/HubAll_#{msu_file_date}.csv"
                    |> Path.expand(__DIR__)

        Extract.Vision.pull_mkt_site_users_hub(instance_name)
        Extract.Vision.pull_mbr_group_hub(instance_name)
        Extract.Vision.pull_contacts(instance_name)
        Extract.Vision.pull_mbr_group_role_hub(instance_name)
        Transform.VisionHub.process_groups(instance_name, msu_file_name)
        Sfdc.Etl.CalcGroupMetrics.calc_contact_metrics(instance_name, msu_file_name)
    #IO.puts DateTime.to_string(DateTime.utc_now())
    #		{:ok, vision_conn} = SftpEx.connect([host: 'upload.visionps.com', user: 'Teladoc', password: 'Qw!2CcskYefr'])
    #
    #		{:ok, msu_file} = File.open(msu_file_name, [:read])
    #		SFTP.TransferService.upload(vision_conn, "/welcomeKits/MBUsers/test_file.csv", msu_file)
    #		#		|> Stream.into(SftpEx.stream!(vision_conn, "/welcomeKits/MBUsers/test_file.csv"))
    #		#		|> Stream.run
    #
    #		SftpEx.disconnect(vision_conn)
    #		File.close(msu_file)
    IO.puts DateTime.to_string(DateTime.utc_now())

    #		IO.puts "Qw!2CcskYefr"
  end
end
