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
          path: "native/ducky_nif",
          mode: :release
        ]
      ],
      aliases: aliases(),
      deps: deps()
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
