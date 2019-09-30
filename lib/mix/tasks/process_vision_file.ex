defmodule Mix.Tasks.ProcessVisionFile do
	use Mix.Task
	def run(_) do
		instance_name = "production"
		{:ok, _} = :application.ensure_all_started(:httpoison)
		{:ok, _} = :application.ensure_all_started(:sfdc_api)

		"#{Application.get_env(:sfdc_api, :etldata_dir)}/#{instance_name}/vision"
		|> Path.expand
		|> File.mkdir

		msu_file_date = Timex.Format.DateTime.Formatters.Strftime.format!(Timex.now(), "%Y_%0m_%0d")
		msu_file_name = "#{Application.get_env(:sfdc_api, :base_dir)}/#{instance_name}/vision/HubAll_#{msu_file_date}.csv"
										|> Path.expand(__DIR__)

		Extract.Vision.pull_odd_duck_msu(instance_name)
		Extract.Vision.pull_mbr_group(instance_name)
		Extract.Vision.pull_contacts(instance_name)

		Transform.Vision.process_groups(instance_name, msu_file_name)

#		IO.puts DateTime.to_string(DateTime.utc_now())
#		IO.puts DateTime.to_string(DateTime.utc_now())

		#		IO.puts "Qw!2CcskYefr"
	end
end
