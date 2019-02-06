defmodule ExAws.Glue do
  @moduledoc """
  Operations on AWS Glue

  https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api.html
  """

  import ExAws.Utils, only: [camelize_keys: 1]

  require Logger

  @type database_name :: String.t()

  @typedoc """
  Table input object:

  https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-tables.html#aws-glue-api-catalog-tables-TableInput
  """
  @type table_input :: map

  @type table_name :: String.t()

  @namespace "AWSGlue"

  #
  # API
  #

  @spec batch_create_partition(database_name, table_name, [{:catalog_id, String.t()}]) ::
          ExAws.Operation.JSON.t()
  def batch_create_partition(database_name, table_name, partitions, opts \\ []) do
    data =
      opts
      |> normalize_opts()
      |> Map.merge(%{
        "DatabaseName" => database_name,
        "PartitionInputList" => partitions,
        "TableName" => table_name
      })

    request(:batch_create_partition, data)
  end

  @spec create_table(database_name, table_input, [{:catalog_id, String.t()}]) ::
          ExAws.Operation.JSON.t()
  def create_table(database_name, table_input, opts \\ []) do
    data =
      opts
      |> normalize_opts()
      |> Map.merge(%{
        "DatabaseName" => database_name,
        "TableInput" => table_input
      })

    request(:create_table, data)
  end

  @spec delete_table(database_name, table_name, [{:catalog_id, String.t()}]) ::
          ExAws.Operation.JSON.t()
  def delete_table(database_name, table_name, opts \\ []) do
    data =
      opts
      |> normalize_opts()
      |> Map.merge(%{
        "DatabaseName" => database_name,
        "Name" => table_name
      })

    request(:delete_table, data)
  end

  @spec get_partitions(database_name, table_name, [{:catalog_id, String.t()}]) ::
          ExAws.Operation.JSON.t()
  def get_partitions(database_name, table_name, opts \\ []) do
    data =
      opts
      |> normalize_opts()
      |> Map.merge(%{
        "DatabaseName" => database_name,
        "TableName" => table_name
      })

    request(:get_partitions, data)
  end

  @spec get_table(database_name, table_name, [{:catalog_id, String.t()}]) ::
          ExAws.Operation.JSON.t()
  def get_table(database_name, table_name, opts \\ []) do
    data =
      opts
      |> normalize_opts()
      |> Map.merge(%{
        "DatabaseName" => database_name,
        "Name" => table_name
      })

    request(:get_table, data)
  end

  #
  # Util
  #

  defp normalize_opts(opts) do
    opts
    |> Map.new()
    |> camelize_keys()
    |> Enum.map(fn {key, value} ->
      if Keyword.keyword?(value) do
        normalized_value = normalize_opts(value)
        {key, normalized_value}
      else
        {key, value}
      end
    end)
    |> Enum.into(%{})
  end

  defp request(action, data, opts \\ %{}) do
    operation =
      action
      |> Atom.to_string()
      |> Macro.camelize()

    ExAws.Operation.JSON.new(
      :glue,
      %{
        data: data,
        headers: [
          {"x-amz-target", "#{@namespace}.#{operation}"},
          {"content-type", "application/x-amz-json-1.1"}
        ]
      }
      |> Map.merge(opts)
    )
  end
end
