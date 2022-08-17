defmodule Wadm.Model.Store do
  require Logger

  defmodule ModelVersion do
    @moduledoc """
    Represents a model version and its associated creation timestamp as an RFC 3339 time
    """
    @type t :: %__MODULE__{
            name: String.t(),
            version: String.t(),
            created: String.t(),
            description: String.t() | nil
          }
    @derive Jason.Encoder
    defstruct [:name, :version, :created, :description]
  end

  defmodule ModelMap do
    @moduledoc """
    A struct that contains the original, unaltered submitted text for an application specification
    alongside its normalized and validated spec
    """
    @type t :: %__MODULE__{
            raw: binary(),
            vetted: Wadm.Model.AppSpec.t()
          }

    @derive Jason.Encoder
    defstruct [:raw, :vetted]
  end

  @type model_map :: Map.t()

  @spec lattice_models(Redix.connection(), String.t()) ::
          {:ok, [ModelVersion.t()]} | {:error, String.t()}
  @doc """
  Returns the most recent version of each model in the given lattice
  """
  def lattice_models(redis, lattice_id) when is_binary(lattice_id) do
    setkey = "wadm:#{lattice_id}:models"

    with {:ok, models} <- Redix.command(redis, ["SMEMBERS", setkey]) do
      {:ok,
       models
       |> Enum.map(fn model ->
         vkey = "wadm:#{lattice_id}:#{model}:versions"

         with {:ok, [ver, stamp]} <-
                Redix.command(redis, ["ZRANGE", vkey, 0, 0, "REV", "WITHSCORES"]),
              {:ok, mm} <- get_model_version(redis, model, ver, lattice_id) do
           %ModelVersion{
             name: model,
             version: ver,
             description: mm.vetted.description,
             created: stamp_to_iso_8601(stamp)
           }
         else
           e ->
             Logger.error("Failed to perform lookup on lattice model #{model}: #{inspect(e)}")
             %ModelVersion{}
         end
       end)}
    else
      e ->
        {:error, "Failed to query models in lattice: #{inspect(e)}"}
    end
  end

  @spec model_versions(Redix.connection(), String.t(), String.t()) ::
          {:ok, [ModelVersion.t()]} | {:error, String.t()}
  @doc """
  Retrieves a list of versions belonging to a given model by spec name. The version
  list returned is sorted in descending order by the time when the version was pushed
  and not the version string itself.
  """
  def model_versions(redis, model_name, lattice_id)
      when is_binary(model_name) and is_binary(lattice_id) do
    setkey = "wadm:#{lattice_id}:#{model_name}:versions"

    with {:ok, versions} <- Redix.command(redis, ["ZRANGE", setkey, 0, -1, "REV", "WITHSCORES"]) do
      {:ok,
       versions
       |> Enum.chunk_every(2)
       |> Enum.map(&List.to_tuple/1)
       |> Enum.map(fn {v, stamp} ->
         %ModelVersion{name: model_name, version: v, created: stamp_to_iso_8601(stamp)}
       end)}
    else
      {:error, e} ->
        {:error, "Failed to query model versions: #{inspect(e)}"}
    end
  end

  @spec put_model_version(Redix.connection(), String.t(), Wadm.Model.AppSpec.t(), String.t()) ::
          :ok | {:error, String.t()}
  @doc """
  Places a model version into the database. If the model already exists then this function will return an error,
  e.g. consumers should check for existence before trying to put. This function uses a pipelined transaction
  so that the add to the range (versions) and the key (details) happen atomically without interference from
  other concurrent requests of the same connection.
  """
  def put_model_version(redis, raw_model, vetted_model = %Wadm.Model.AppSpec{}, lattice_id)
      when is_binary(raw_model) do
    with {:ok, versions} <- model_versions(redis, vetted_model.name, lattice_id) do
      if versions |> Enum.filter(fn v -> v.version == vetted_model.version end) |> Enum.count() >
           0 do
        {:error,
         "Version #{vetted_model.version} is already stored for model #{vetted_model.name}"}
      else
        lattice_set_key = "wadm:#{lattice_id}:models"
        set_key = "wadm:#{lattice_id}:#{vetted_model.name}:versions"
        detail_key = "wadm:#{lattice_id}:#{vetted_model.name}:versions:#{vetted_model.version}"

        model_map =
          %ModelMap{
            raw: raw_model,
            vetted: vetted_model
          }
          |> :erlang.term_to_binary()

        with {:ok, [1, "OK", _ct]} <-
               Redix.transaction_pipeline(
                 redis,
                 [
                   ["ZADD", set_key, now_to_stamp(), vetted_model.version],
                   ["SET", detail_key, model_map],
                   ["SADD", lattice_set_key, vetted_model.name]
                 ]
               ) do
          :ok
        else
          e ->
            {:error, "Failed to add model version to Redis: #{inspect(e)}"}
        end
      end
    else
      {:error, e} ->
        {:error, e}
    end
  end

  @spec delete_model_version(Redix.connection(), String.t(), String.t(), String.t()) ::
          :ok | {:error, String.t()}
  @doc """
  This function removes a model version from the persistent store.
  """
  def delete_model_version(redis, model_name, version, lattice_id)
      when is_binary(model_name) and is_binary(version) and is_binary(lattice_id) do
    set_key = "wadm:#{lattice_id}:#{model_name}:versions"
    detail_key = "wadm:#{lattice_id}:#{model_name}:versions:#{version}"

    with {:ok, [1, 1]} <-
           Redix.transaction_pipeline(
             redis,
             [
               ["ZREM", set_key, version],
               ["DEL", detail_key]
             ]
           ) do
      clean_model_name(redis, model_name, lattice_id)
      :ok
    else
      {:ok, [0, 0]} ->
        Logger.warning("Attempt to remove a non-existent model version from the store")
        :ok

      e ->
        {:error, "Failed to delete model version from Redis: #{inspect(e)}"}
    end
  end

  defp clean_model_name(redis, model_name, lattice_id) do
    set_key = "wadm:#{lattice_id}:models"

    with {:ok, []} <- model_versions(redis, model_name, lattice_id),
         {:ok, 1} <- Redix.command(redis, ["SREM", set_key, model_name]) do
      Logger.debug("Removed model name #{model_name} from store (no more versions)")
    end
  end

  @spec get_model_version(Redix.connection(), String.t(), String.t(), String.t()) ::
          {:ok, ModelMap.t()} | {:error, String.t()}
  @doc """
  This function retrieves the details for a specific model version. This includes
  both the originally submitted raw text and the post-validation, normalized model
  """
  def get_model_version(redis, model_name, version, lattice_id)
      when is_binary(model_name) and is_binary(version) and is_binary(lattice_id) do
    detail_key = "wadm:#{lattice_id}:#{model_name}:versions:#{version}"

    with {:ok, raw} <- Redix.command(redis, ["GET", detail_key]) do
      if raw != nil do
        {:ok, raw |> :erlang.binary_to_term()}
      else
        {:error, "No such model version in store: #{model_name} #{version}"}
      end
    else
      e ->
        {:error, "Failed to retrieve model version: #{inspect(e)}"}
    end
  end

  @spec model_version_exists?(Redix.connection(), String.t(), String.t(), String.t()) :: boolean()
  @doc """
  Perform a safe check to see if a model version exists. Consumers should call this to verify
  the existence of a model version before performing other actions that might fail on non-existent
  version details
  """
  def model_version_exists?(redis, model_name, version, lattice_id) do
    detail_key = "wadm:#{lattice_id}:#{model_name}:versions:#{version}"

    with {:ok, 1} <- Redix.command(redis, ["EXISTS", detail_key]) do
      true
    else
      _ ->
        false
    end
  end

  defp stamp_to_iso_8601(stamp) when is_integer(stamp) do
    with {:ok, dt} <- DateTime.from_unix(stamp, :millisecond) do
      dt |> DateTime.to_iso8601()
    else
      _ ->
        Logger.error("Failed to generate unix timestamp from #{stamp}")
        "N/A"
    end
  end

  defp stamp_to_iso_8601(stamp) when is_binary(stamp) do
    stamp |> Integer.parse() |> elem(0) |> stamp_to_iso_8601()
  end

  defp now_to_stamp() do
    DateTime.utc_now() |> DateTime.to_unix(:millisecond)
  end
end
