defmodule Ducky.MixProject do
  use Mix.Project

  def project do
    [
      app: :ducky,
      version: "0.1.0",
      elixir: "~> 1.14",
      erlc_paths: ["src", "build/dev/erlang"],
      compilers: [:gleam, :rustler] ++ Mix.compilers(),
      rustler_crates: [
        ducky_nif: [
          path: "priv/ducky_nif",
          mode: :release
        ]
      ],
      package: package(),
      aliases: aliases(),
      deps: deps()
    ]
  end

  defp package do
    [
      files: ~w(
        src
        priv
        gleam.toml
        LICENSE
        README.md
        CHANGELOG.md
      ),
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => "https://github.com/lemorage/ducky"}
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      {:rustler, "~> 0.37.0", runtime: false}
    ]
  end

  defp aliases do
    [
      "compile.gleam": &compile_gleam/1
    ]
  end

  defp compile_gleam(_args) do
    Mix.shell().cmd("gleam build")
  end
end
