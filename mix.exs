defmodule SupsUp.MixProject do
	use Mix.Project

	def project do
		[
			app: :sups_up,
			version: "0.1.0",
			elixir: "~> 1.7",
			start_permanent: Mix.env() == :prod,
			deps: deps()
		]
	end

	# Run "mix help compile.app" to learn about applications.
	def application do
		[
			extra_applications: [:logger, :sfdc_api, :timex, :sftp_ex],
			mod: {SupsUp.Application, []}
		]
	end

	# Run "mix help deps" to learn about dependencies.
	defp deps do
		[
			{:sftp_ex, "~> 0.2.6"},
			{:csv, "~> 2.1"},
			{:timex, "~> 3.3.0"},
			{:sfdc_api, git: "git@github.com:martinee6474/sfdc-api.git"}
		]
	end
end
